/*
 * asterism-zenoh: Asterism::Zenoh for CRuby, over zenoh-c.
 *
 * The Ruby API is the one of the mruby / PicoRuby gem
 * (ruby-asterism/picoruby-asterism-zenoh, over zenoh-pico): sessions
 * (client, or peer with an optional listener), put / subscribe, get /
 * queryable (query and reply), liveliness tokens and watches, attachments,
 * Session#zid. Code written for one runs on the other.
 *
 * Threading model: zenoh-c runs the protocol on its own threads and calls
 * the callbacks below from them. No callback touches Ruby (no GVL, no Ruby
 * object, no Ruby allocator). Each subscriber, liveliness watch, queryable
 * and get owns a channel (struct zch): zenoh-c's FIFO channel holds what was
 * received, and the gem's callback in front of it keeps the counters and
 * drops the oldest entry when the channel is full (as the mruby gem's ring
 * does; zenoh-c's own FIFO would block zenoh's thread instead). Ruby takes
 * the entries out with each_pending / each_reply from the application's own
 * thread. Session#poll only checks the connection: receiving needs no poll.
 *
 * The GVL is released while zenoh-c may wait: opening a session (connecting
 * to the router), put, get, liveliness_get and close. A session may be used
 * from several Ruby threads: the calls that hold the GVL are serialized by
 * it, the ones without it by the session's op_lock, and closing marks the
 * session closed for Ruby (with the GVL held) before zenoh-c drops it.
 *
 * Losing the connection (same rules as the mruby gem): a client session is
 * closed when it has no router left, a peer session that only connects when
 * it has no peer left; a listening peer session stays open. The check runs
 * in poll, closed?, put and get. From then on poll returns false, closed?
 * is true and put raises Asterism::Zenoh::Error. There is no reconnection.
 *
 * Lifetime: closing is optional. A session keeps lists of its live
 * subscribers, queryables and tokens and undeclares them before it closes,
 * so they can be freed in any order. A channel is owned jointly by its Ruby
 * object and zenoh-c (which drops the gem's closure when the entity is
 * undeclared or the get is finished); whichever lets go last frees it.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <ruby.h>
#include <ruby/encoding.h>
#include <ruby/thread.h>

#include <zenoh.h>

#define ZRB_DEFAULT_DEPTH 16
#define ZRB_MAX_DEPTH 1024
#define ZRB_DEFAULT_POLL_STEPS 8
#define ZRB_DEFAULT_GET_TIMEOUT_MS 2000
#define ZRB_MAX_GET_TIMEOUT_MS 600000
/* Same values as the mruby gem's PICORUBY_ZENOH_*_MS. */
#define ZRB_CONNECT_TIMEOUT_MS 3000
#define ZRB_SEND_TIMEOUT_MS 3000

static VALUE mAsterism, mZenoh, eZenohError;
static VALUE cSession, cSubscriber, cWatch, cQueryable, cQuery, cGet, cToken;
static ID id_attachment, id_target, id_consolidation, id_complete, id_mode, id_listen;
static ID id_iv_session, id_iv_key;
static ID id_client, id_peer, id_all, id_all_complete, id_best_matching;
static ID id_none, id_latest, id_monotonic, id_auto;

/* ------------------------------------------------------------- channels */

typedef enum { ZCH_SAMPLE, ZCH_REPLY, ZCH_QUERY } zch_kind;

/* What zenoh-c received for one Ruby object, waiting to be taken. */
typedef struct {
    zch_kind kind;
    pthread_mutex_t lock; /* the gem's callback, and Ruby taking an entry */
    uint32_t depth;
    atomic_uint queued;   /* entries in the FIFO */
    atomic_uint received;
    atomic_uint dropped;  /* oldest entries dropped because the FIFO was full */
    atomic_uint errors;   /* error replies (gets) */
    atomic_bool done;     /* zenoh-c dropped the gem's closure */
    atomic_int refs;      /* the Ruby object and zenoh-c's closure */
    union {
        z_owned_closure_sample_t sample;
        z_owned_closure_reply_t reply;
        z_owned_closure_query_t query;
    } tx; /* the FIFO's sending end */
    union {
        z_owned_fifo_handler_sample_t sample;
        z_owned_fifo_handler_reply_t reply;
        z_owned_fifo_handler_query_t query;
    } rx; /* the FIFO's receiving end */
} zch;

static zch *zch_new(zch_kind kind, uint32_t depth) {
    zch *c = (zch *)calloc(1, sizeof(zch));
    if (c == NULL) {
        rb_raise(rb_eNoMemError, "cannot allocate a zenoh channel");
    }
    c->kind = kind;
    c->depth = depth;
    pthread_mutex_init(&c->lock, NULL);
    atomic_init(&c->queued, 0);
    atomic_init(&c->received, 0);
    atomic_init(&c->dropped, 0);
    atomic_init(&c->errors, 0);
    atomic_init(&c->done, false);
    atomic_init(&c->refs, 1); /* the Ruby side; zch_closure_* adds zenoh's */
    switch (kind) {
    case ZCH_SAMPLE:
        z_fifo_channel_sample_new(&c->tx.sample, &c->rx.sample, depth);
        break;
    case ZCH_REPLY:
        z_fifo_channel_reply_new(&c->tx.reply, &c->rx.reply, depth);
        break;
    case ZCH_QUERY:
        z_fifo_channel_query_new(&c->tx.query, &c->rx.query, depth);
        break;
    }
    return c;
}

static void zch_unref(zch *c) {
    if (c == NULL || atomic_fetch_sub(&c->refs, 1) != 1) {
        return;
    }
    /* Dropping the receiving end drops what is left (a query left there
     * sends its final reply). */
    switch (c->kind) {
    case ZCH_SAMPLE:
        z_drop(z_move(c->tx.sample));
        z_drop(z_move(c->rx.sample));
        break;
    case ZCH_REPLY:
        z_drop(z_move(c->tx.reply));
        z_drop(z_move(c->rx.reply));
        break;
    case ZCH_QUERY:
        z_drop(z_move(c->tx.query));
        z_drop(z_move(c->rx.query));
        break;
    }
    pthread_mutex_destroy(&c->lock);
    free(c);
}

/* zenoh-c is done with the gem's closure: the entity was undeclared, or
 * the get finished (every reply in, the time ran out, the session closed). */
static void zch_on_drop(void *ctx) {
    zch *c = (zch *)ctx;
    atomic_store(&c->done, true);
    zch_unref(c);
}

/* Called with c->lock held, from zenoh's thread. Makes room for one entry
 * by dropping the oldest when the FIFO is full, so sending never blocks. */
static void zch_make_room(zch *c) {
    if (atomic_load(&c->queued) < c->depth) {
        return;
    }
    z_result_t r = Z_CHANNEL_NODATA;
    switch (c->kind) {
    case ZCH_SAMPLE: {
        z_owned_sample_t old;
        r = z_fifo_handler_sample_try_recv(z_loan(c->rx.sample), &old);
        if (r == Z_OK) {
            z_drop(z_move(old));
        }
        break;
    }
    case ZCH_REPLY: {
        z_owned_reply_t old;
        r = z_fifo_handler_reply_try_recv(z_loan(c->rx.reply), &old);
        if (r == Z_OK) {
            z_drop(z_move(old));
        }
        break;
    }
    case ZCH_QUERY: {
        /* Finishing the oldest unanswered query (its final reply goes out). */
        z_owned_query_t old;
        r = z_fifo_handler_query_try_recv(z_loan(c->rx.query), &old);
        if (r == Z_OK) {
            z_drop(z_move(old));
        }
        break;
    }
    }
    if (r == Z_OK) {
        atomic_fetch_sub(&c->queued, 1);
        atomic_fetch_add(&c->dropped, 1);
    }
}

/* The gem's callbacks, on zenoh's threads. They never touch Ruby. */
static void zch_on_sample(z_loaned_sample_t *sample, void *ctx) {
    zch *c = (zch *)ctx;
    pthread_mutex_lock(&c->lock);
    zch_make_room(c);
    z_closure_sample_call(z_loan(c->tx.sample), sample);
    atomic_fetch_add(&c->queued, 1);
    atomic_fetch_add(&c->received, 1);
    pthread_mutex_unlock(&c->lock);
}

static void zch_on_reply(z_loaned_reply_t *reply, void *ctx) {
    zch *c = (zch *)ctx;
    if (!z_reply_is_ok(reply)) {
        atomic_fetch_add(&c->errors, 1);
        return;
    }
    pthread_mutex_lock(&c->lock);
    zch_make_room(c);
    z_closure_reply_call(z_loan(c->tx.reply), reply);
    atomic_fetch_add(&c->queued, 1);
    atomic_fetch_add(&c->received, 1);
    pthread_mutex_unlock(&c->lock);
}

static void zch_on_query(z_loaned_query_t *query, void *ctx) {
    zch *c = (zch *)ctx;
    pthread_mutex_lock(&c->lock);
    zch_make_room(c);
    z_closure_query_call(z_loan(c->tx.query), query);
    atomic_fetch_add(&c->queued, 1);
    atomic_fetch_add(&c->received, 1);
    pthread_mutex_unlock(&c->lock);
}

/* The closure handed to zenoh-c; it holds a reference to c. */
static void zch_closure_sample(zch *c, z_owned_closure_sample_t *cb) {
    atomic_fetch_add(&c->refs, 1);
    z_closure_sample(cb, zch_on_sample, zch_on_drop, c);
}

static void zch_closure_reply(zch *c, z_owned_closure_reply_t *cb) {
    atomic_fetch_add(&c->refs, 1);
    z_closure_reply(cb, zch_on_reply, zch_on_drop, c);
}

static void zch_closure_query(zch *c, z_owned_closure_query_t *cb) {
    atomic_fetch_add(&c->refs, 1);
    z_closure_query(cb, zch_on_query, zch_on_drop, c);
}

/* Ruby side: take one entry (Z_OK) or nothing (Z_CHANNEL_NODATA). */
static z_result_t zch_take_sample(zch *c, z_owned_sample_t *out) {
    pthread_mutex_lock(&c->lock);
    z_result_t r = z_fifo_handler_sample_try_recv(z_loan(c->rx.sample), out);
    if (r == Z_OK) {
        atomic_fetch_sub(&c->queued, 1);
    }
    pthread_mutex_unlock(&c->lock);
    return r;
}

static z_result_t zch_take_reply(zch *c, z_owned_reply_t *out) {
    pthread_mutex_lock(&c->lock);
    z_result_t r = z_fifo_handler_reply_try_recv(z_loan(c->rx.reply), out);
    if (r == Z_OK) {
        atomic_fetch_sub(&c->queued, 1);
    }
    pthread_mutex_unlock(&c->lock);
    return r;
}

static z_result_t zch_take_query(zch *c, z_owned_query_t *out) {
    pthread_mutex_lock(&c->lock);
    z_result_t r = z_fifo_handler_query_try_recv(z_loan(c->rx.query), out);
    if (r == Z_OK) {
        atomic_fetch_sub(&c->queued, 1);
    }
    pthread_mutex_unlock(&c->lock);
    return r;
}

/* Finish every query still waiting (each sends its final reply). */
static void zch_finish_queries(zch *c) {
    z_owned_query_t q;
    while (zch_take_query(c, &q) == Z_OK) {
        z_drop(z_move(q));
    }
}

/* -------------------------------------------------------------- helpers */

static VALUE zrb_str_from_view(const z_loaned_string_t *s) {
    return rb_utf8_str_new(z_string_data(s), (long)z_string_len(s));
}

static VALUE zrb_key_str(const z_loaned_keyexpr_t *ke) {
    z_view_string_t ks;
    z_keyexpr_as_view_string(ke, &ks);
    return zrb_str_from_view(z_loan(ks));
}

/* Bytes as a binary String (they are not necessarily text). */
static VALUE zrb_str_from_bytes(const z_loaned_bytes_t *b) {
    size_t len = z_bytes_len(b);
    VALUE str = rb_str_new(NULL, (long)len);
    z_bytes_reader_t reader = z_bytes_get_reader(b);
    size_t got = z_bytes_reader_read(&reader, (uint8_t *)RSTRING_PTR(str), len);
    if (got != len) {
        rb_str_set_len(str, (long)got);
    }
    return str;
}

/* nil when there is no attachment, or an empty one (as the mruby gem). */
static VALUE zrb_attachment_value(const z_loaned_bytes_t *b) {
    if (b == NULL || z_bytes_len(b) == 0) {
        return Qnil;
    }
    return zrb_str_from_bytes(b);
}

static void zrb_view_key(z_view_keyexpr_t *ke, const char *key) {
    if (z_view_keyexpr_from_str(ke, key) != Z_OK) {
        rb_raise(rb_eArgError, "invalid key expression: %s", key);
    }
}

/* An owned copy of the key, for calls made without the GVL. */
static void zrb_owned_key(z_owned_keyexpr_t *ke, const char *key) {
    if (z_keyexpr_from_str(ke, key) != Z_OK) {
        rb_raise(rb_eArgError, "invalid key expression: %s", key);
    }
}

static long zrb_check_depth(VALUE v) {
    long depth = NIL_P(v) ? ZRB_DEFAULT_DEPTH : NUM2LONG(v);
    if (depth < 1 || depth > ZRB_MAX_DEPTH) {
        rb_raise(rb_eArgError, "depth must be 1..%d", ZRB_MAX_DEPTH);
    }
    return depth;
}

static long zrb_check_timeout(VALUE v) {
    long t = NIL_P(v) ? ZRB_DEFAULT_GET_TIMEOUT_MS : NUM2LONG(v);
    if (t < 1 || t > ZRB_MAX_GET_TIMEOUT_MS) {
        rb_raise(rb_eArgError, "timeout must be 1..%d ms", ZRB_MAX_GET_TIMEOUT_MS);
    }
    return t;
}

/* The attachment: keyword: nil or a String. */
static VALUE zrb_kw_attachment(VALUE v) {
    if (v == Qundef || NIL_P(v)) {
        return Qnil;
    }
    if (!RB_TYPE_P(v, T_STRING)) {
        rb_raise(rb_eTypeError, "attachment must be a String or nil");
    }
    return v;
}

static void zrb_bytes_from_str(z_owned_bytes_t *out, VALUE str, const char *what) {
    if (z_bytes_copy_from_buf(out, (const uint8_t *)RSTRING_PTR(str), (size_t)RSTRING_LEN(str)) != Z_OK) {
        rb_raise(eZenohError, "cannot allocate the %s", what);
    }
}

/* A Symbol (or String) keyword value as an ID. */
static ID zrb_sym_id(VALUE v) {
    if (RB_TYPE_P(v, T_STRING)) {
        return rb_intern_str(v);
    }
    if (SYMBOL_P(v)) {
        return SYM2ID(v);
    }
    rb_raise(rb_eTypeError, "expected a Symbol, got %" PRIsVALUE, rb_obj_class(v));
    return 0; /* not reached */
}

/* No .new: the objects come from Session.open and the Session methods (as in
 * the mruby gem, where new is undefined too). */
static void zrb_no_new(VALUE klass) {
    rb_undef_alloc_func(klass);
    rb_undef_method(rb_singleton_class(klass), "new");
}

/* Keep the session object alive while obj is reachable. */
static void zrb_hold_session(VALUE obj, VALUE session, VALUE key) {
    rb_ivar_set(obj, id_iv_session, session);
    if (!NIL_P(key)) {
        rb_ivar_set(obj, id_iv_key, rb_str_dup(key));
    }
}

/* ------------------------------------------------------- object structs */

typedef struct zrb_session zrb_session;
typedef struct zrb_sub zrb_sub;
typedef struct zrb_qable zrb_qable;
typedef struct zrb_token zrb_token;

struct zrb_session {
    z_owned_session_t session;
    pthread_mutex_t op_lock; /* calls made without the GVL, and close */
    bool open;      /* Ruby's view; changed only with the GVL held */
    bool live;      /* the zenoh-c session exists; changed under op_lock */
    bool peer;      /* peer mode (otherwise client) */
    bool listening; /* peer mode with a listener: stays open without peers */
    zrb_sub *subs;  /* live subscribers and liveliness watches */
    zrb_qable *qables;
    zrb_token *tokens;
};

struct zrb_sub {
    z_owned_subscriber_t sub;
    bool declared;
    bool liveliness;
    zrb_session *owner; /* NULL once detached */
    zrb_sub *next;
    zch *ch;
};

struct zrb_qable {
    z_owned_queryable_t qable;
    bool declared;
    zrb_session *owner;
    zrb_qable *next;
    zch *ch;
};

struct zrb_token {
    z_owned_liveliness_token_t token;
    bool declared;
    zrb_session *owner;
    zrb_token *next;
};

typedef struct {
    zch *ch;
} zrb_get;

typedef struct {
    z_owned_query_t query;
    bool live;
} zrb_query;

/* ----------------------------------------------------------- subscriber */

static void zrb_sub_detach(zrb_sub *s) {
    if (s->declared) {
        z_drop(z_move(s->sub));
        s->declared = false;
    }
    if (s->owner != NULL) {
        zrb_sub **pp = &s->owner->subs;
        while (*pp != NULL) {
            if (*pp == s) {
                *pp = s->next;
                break;
            }
            pp = &(*pp)->next;
        }
        s->owner = NULL;
        s->next = NULL;
    }
}

static void zrb_sub_free(void *p) {
    zrb_sub *s = (zrb_sub *)p;
    zrb_sub_detach(s);
    zch_unref(s->ch);
    xfree(s);
}

static size_t zrb_sub_size(const void *p) { return sizeof(zrb_sub); }

static const rb_data_type_t zrb_sub_type = {
    "Asterism::Zenoh::Subscriber", {NULL, zrb_sub_free, zrb_sub_size}, NULL, NULL, 0};

static zrb_sub *zrb_sub_get(VALUE self) {
    zrb_sub *s;
    TypedData_Get_Struct(self, zrb_sub, &zrb_sub_type, s);
    return s;
}

/* Take out what is pending now, oldest first. Values that arrive while the
 * block runs are left for the next call. */
static VALUE zrb_sub_each_pending(VALUE self) {
    zrb_sub *s = zrb_sub_get(self);
    bool collect = !rb_block_given_p();
    VALUE out = collect ? rb_ary_new() : Qnil;
    long taken = 0;
    unsigned todo = atomic_load(&s->ch->queued);
    while (todo > 0) {
        todo--;
        z_owned_sample_t sm;
        if (zch_take_sample(s->ch, &sm) != Z_OK) {
            break;
        }
        const z_loaned_sample_t *l = z_loan(sm);
        VALUE vals[3];
        int n;
        vals[0] = zrb_key_str(z_sample_keyexpr(l));
        if (s->liveliness) {
            /* A token appearing is a PUT, one going away a DELETE. */
            vals[1] = (z_sample_kind(l) == Z_SAMPLE_KIND_PUT) ? Qtrue : Qfalse;
            n = 2;
        } else {
            vals[1] = zrb_str_from_bytes(z_sample_payload(l));
            vals[2] = zrb_attachment_value(z_sample_attachment(l));
            n = 3;
        }
        z_drop(z_move(sm));
        taken++;
        if (collect) {
            rb_ary_push(out, rb_ary_new_from_values(n, vals));
        } else {
            rb_yield_values2(n, vals);
        }
    }
    return collect ? out : LONG2NUM(taken);
}

static VALUE zrb_sub_pending(VALUE self) { return UINT2NUM(atomic_load(&zrb_sub_get(self)->ch->queued)); }
static VALUE zrb_sub_received(VALUE self) { return UINT2NUM(atomic_load(&zrb_sub_get(self)->ch->received)); }
static VALUE zrb_sub_dropped(VALUE self) { return UINT2NUM(atomic_load(&zrb_sub_get(self)->ch->dropped)); }

static VALUE zrb_sub_close(VALUE self) {
    zrb_sub_detach(zrb_sub_get(self));
    return Qnil;
}

static VALUE zrb_sub_closed_p(VALUE self) { return zrb_sub_get(self)->declared ? Qfalse : Qtrue; }

/* ------------------------------------------------------------ queryable */

static void zrb_qable_detach(zrb_qable *q) {
    /* Unanswered queries first: their final replies go out while the
     * session can still carry them. */
    zch_finish_queries(q->ch);
    if (q->declared) {
        z_drop(z_move(q->qable));
        q->declared = false;
    }
    if (q->owner != NULL) {
        zrb_qable **pp = &q->owner->qables;
        while (*pp != NULL) {
            if (*pp == q) {
                *pp = q->next;
                break;
            }
            pp = &(*pp)->next;
        }
        q->owner = NULL;
        q->next = NULL;
    }
}

static void zrb_qable_free(void *p) {
    zrb_qable *q = (zrb_qable *)p;
    zrb_qable_detach(q);
    zch_unref(q->ch);
    xfree(q);
}

static size_t zrb_qable_size(const void *p) { return sizeof(zrb_qable); }

static const rb_data_type_t zrb_qable_type = {
    "Asterism::Zenoh::Queryable", {NULL, zrb_qable_free, zrb_qable_size}, NULL, NULL, 0};

static zrb_qable *zrb_qable_get(VALUE self) {
    zrb_qable *q;
    TypedData_Get_Struct(self, zrb_qable, &zrb_qable_type, q);
    return q;
}

/* ---------------------------------------------------------------- query */

static void zrb_query_finish(zrb_query *zq) {
    if (zq->live) {
        z_drop(z_move(zq->query));
        zq->live = false;
    }
}

static void zrb_query_free(void *p) {
    zrb_query *zq = (zrb_query *)p;
    zrb_query_finish(zq);
    xfree(zq);
}

static size_t zrb_query_size(const void *p) { return sizeof(zrb_query); }

static const rb_data_type_t zrb_query_type = {
    "Asterism::Zenoh::Query", {NULL, zrb_query_free, zrb_query_size}, NULL, NULL, 0};

static zrb_query *zrb_query_get(VALUE self) {
    zrb_query *zq;
    TypedData_Get_Struct(self, zrb_query, &zrb_query_type, zq);
    return zq;
}

static zrb_query *zrb_query_get_live(VALUE self) {
    zrb_query *zq = zrb_query_get(self);
    if (!zq->live) {
        rb_raise(eZenohError, "the query is finished");
    }
    return zq;
}

/* Wraps the query (moved in) in a Query object. */
static VALUE zrb_query_wrap(z_owned_query_t *q) {
    zrb_query *zq;
    VALUE obj = TypedData_Make_Struct(cQuery, zrb_query, &zrb_query_type, zq);
    zq->query = *q;
    zq->live = true;
    z_internal_query_null(q);
    return obj;
}

static VALUE zrb_query_yield_body(VALUE obj) { return rb_yield(obj); }

static VALUE zrb_query_yield_ensure(VALUE obj) {
    zrb_query_finish(zrb_query_get(obj));
    return Qnil;
}

/* qa.each_pending { |q| ... } -> Integer (queries taken). Each query is
 * finished when the block returns (also when it raises).
 * qa.each_pending -> Array of Query; each stays open until Query#finish or
 * garbage collection. */
static VALUE zrb_qable_each_pending(VALUE self) {
    zrb_qable *q = zrb_qable_get(self);
    bool collect = !rb_block_given_p();
    VALUE out = collect ? rb_ary_new() : Qnil;
    long taken = 0;
    unsigned todo = atomic_load(&q->ch->queued);
    while (todo > 0) {
        todo--;
        z_owned_query_t oq;
        if (zch_take_query(q->ch, &oq) != Z_OK) {
            break;
        }
        VALUE obj = zrb_query_wrap(&oq);
        taken++;
        if (collect) {
            rb_ary_push(out, obj);
        } else {
            rb_ensure(zrb_query_yield_body, obj, zrb_query_yield_ensure, obj);
        }
    }
    return collect ? out : LONG2NUM(taken);
}

static VALUE zrb_qable_pending(VALUE self) { return UINT2NUM(atomic_load(&zrb_qable_get(self)->ch->queued)); }
static VALUE zrb_qable_received(VALUE self) { return UINT2NUM(atomic_load(&zrb_qable_get(self)->ch->received)); }
static VALUE zrb_qable_dropped(VALUE self) { return UINT2NUM(atomic_load(&zrb_qable_get(self)->ch->dropped)); }

static VALUE zrb_qable_close(VALUE self) {
    zrb_qable_detach(zrb_qable_get(self));
    return Qnil;
}

static VALUE zrb_qable_closed_p(VALUE self) { return zrb_qable_get(self)->declared ? Qfalse : Qtrue; }

static VALUE zrb_query_key(VALUE self) {
    zrb_query *zq = zrb_query_get_live(self);
    return zrb_key_str(z_query_keyexpr(z_loan(zq->query)));
}

static VALUE zrb_query_params(VALUE self) {
    zrb_query *zq = zrb_query_get_live(self);
    z_view_string_t ps;
    z_query_parameters(z_loan(zq->query), &ps);
    return zrb_str_from_view(z_loan(ps));
}

static VALUE zrb_query_payload(VALUE self) {
    zrb_query *zq = zrb_query_get_live(self);
    const z_loaned_bytes_t *b = z_query_payload(z_loan(zq->query));
    if (b == NULL) {
        return rb_str_new(NULL, 0);
    }
    return zrb_str_from_bytes(b);
}

static VALUE zrb_query_attachment(VALUE self) {
    zrb_query *zq = zrb_query_get_live(self);
    return zrb_attachment_value(z_query_attachment(z_loan(zq->query)));
}

/* q.reply(payload, attachment: nil) / q.reply(key, payload, attachment: nil)
 * -> nil. The key defaults to the query's key; it must match the query's key
 * expression. May be called more than once before the query is finished. */
static VALUE zrb_query_reply(int argc, VALUE *argv, VALUE self) {
    VALUE a1, a2 = Qnil, opts = Qnil;
    int n = rb_scan_args(argc, argv, "11:", &a1, &a2, &opts);
    VALUE kw[1] = {Qundef};
    if (!NIL_P(opts)) {
        rb_get_kwargs(opts, &id_attachment, 0, 1, kw);
    }
    VALUE att = zrb_kw_attachment(kw[0]);
    zrb_query *zq = zrb_query_get_live(self);
    VALUE key_v = (n == 1) ? Qnil : a1;
    VALUE payload = (n == 1) ? a1 : a2;
    if (!RB_TYPE_P(payload, T_STRING)) {
        rb_raise(rb_eTypeError, "payload must be a String");
    }
    z_view_keyexpr_t ke;
    const z_loaned_keyexpr_t *kp;
    if (NIL_P(key_v)) {
        kp = z_query_keyexpr(z_loan(zq->query));
    } else {
        zrb_view_key(&ke, StringValueCStr(key_v));
        kp = z_loan(ke);
    }
    z_query_reply_options_t ro;
    z_query_reply_options_default(&ro);
    z_owned_bytes_t att_bytes;
    if (!NIL_P(att)) {
        zrb_bytes_from_str(&att_bytes, att, "attachment");
        ro.attachment = z_move(att_bytes);
    }
    z_owned_bytes_t bytes;
    if (z_bytes_copy_from_buf(&bytes, (const uint8_t *)RSTRING_PTR(payload), (size_t)RSTRING_LEN(payload)) != Z_OK) {
        if (!NIL_P(att)) {
            z_drop(z_move(att_bytes));
        }
        rb_raise(eZenohError, "cannot allocate the payload");
    }
    z_result_t ret = z_query_reply(z_loan(zq->query), kp, z_move(bytes), &ro);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "reply failed (%d)", (int)ret);
    }
    return Qnil;
}

static VALUE zrb_query_finish_m(VALUE self) {
    zrb_query_finish(zrb_query_get(self));
    return Qnil;
}

static VALUE zrb_query_finished_p(VALUE self) { return zrb_query_get(self)->live ? Qfalse : Qtrue; }

/* ------------------------------------------------------------------ get */

static void zrb_get_free(void *p) {
    zrb_get *g = (zrb_get *)p;
    zch_unref(g->ch); /* zenoh-c may still hold it until the get finishes */
    xfree(g);
}

static size_t zrb_get_size(const void *p) { return sizeof(zrb_get); }

static const rb_data_type_t zrb_get_type = {"Asterism::Zenoh::Get", {NULL, zrb_get_free, zrb_get_size}, NULL, NULL, 0};

static zrb_get *zrb_get_get(VALUE self) {
    zrb_get *g;
    TypedData_Get_Struct(self, zrb_get, &zrb_get_type, g);
    return g;
}

static VALUE zrb_get_new(zrb_get **out) {
    zrb_get *g;
    VALUE obj = TypedData_Make_Struct(cGet, zrb_get, &zrb_get_type, g);
    g->ch = zch_new(ZCH_REPLY, ZRB_DEFAULT_DEPTH);
    *out = g;
    return obj;
}

/* g.each_reply { |key, payload, attachment| ... } -> Integer; without a
 * block, Array of [key, payload, attachment]. Error replies are counted
 * (errors), not yielded. */
static VALUE zrb_get_each_reply(VALUE self) {
    zrb_get *g = zrb_get_get(self);
    bool collect = !rb_block_given_p();
    VALUE out = collect ? rb_ary_new() : Qnil;
    long taken = 0;
    unsigned todo = atomic_load(&g->ch->queued);
    while (todo > 0) {
        todo--;
        z_owned_reply_t r;
        if (zch_take_reply(g->ch, &r) != Z_OK) {
            break;
        }
        VALUE vals[3];
        const z_loaned_sample_t *sm = z_reply_ok(z_loan(r));
        vals[0] = zrb_key_str(z_sample_keyexpr(sm));
        vals[1] = zrb_str_from_bytes(z_sample_payload(sm));
        vals[2] = zrb_attachment_value(z_sample_attachment(sm));
        z_drop(z_move(r));
        taken++;
        if (collect) {
            rb_ary_push(out, rb_ary_new_from_values(3, vals));
        } else {
            rb_yield_values2(3, vals);
        }
    }
    return collect ? out : LONG2NUM(taken);
}

static VALUE zrb_get_done_p(VALUE self) { return atomic_load(&zrb_get_get(self)->ch->done) ? Qtrue : Qfalse; }
static VALUE zrb_get_pending(VALUE self) { return UINT2NUM(atomic_load(&zrb_get_get(self)->ch->queued)); }
static VALUE zrb_get_received(VALUE self) { return UINT2NUM(atomic_load(&zrb_get_get(self)->ch->received)); }
static VALUE zrb_get_dropped(VALUE self) { return UINT2NUM(atomic_load(&zrb_get_get(self)->ch->dropped)); }
static VALUE zrb_get_errors(VALUE self) { return UINT2NUM(atomic_load(&zrb_get_get(self)->ch->errors)); }

/* ---------------------------------------------------------------- token */

static void zrb_token_detach(zrb_token *t) {
    if (t->declared) {
        z_drop(z_move(t->token));
        t->declared = false;
    }
    if (t->owner != NULL) {
        zrb_token **pp = &t->owner->tokens;
        while (*pp != NULL) {
            if (*pp == t) {
                *pp = t->next;
                break;
            }
            pp = &(*pp)->next;
        }
        t->owner = NULL;
        t->next = NULL;
    }
}

static void zrb_token_free(void *p) {
    zrb_token *t = (zrb_token *)p;
    zrb_token_detach(t);
    xfree(t);
}

static size_t zrb_token_size(const void *p) { return sizeof(zrb_token); }

static const rb_data_type_t zrb_token_type = {
    "Asterism::Zenoh::LivelinessToken", {NULL, zrb_token_free, zrb_token_size}, NULL, NULL, 0};

static zrb_token *zrb_token_get(VALUE self) {
    zrb_token *t;
    TypedData_Get_Struct(self, zrb_token, &zrb_token_type, t);
    return t;
}

static VALUE zrb_token_close(VALUE self) {
    zrb_token_detach(zrb_token_get(self));
    return Qnil;
}

static VALUE zrb_token_closed_p(VALUE self) { return zrb_token_get(self)->declared ? Qfalse : Qtrue; }

/* -------------------------------------------------------------- session */

/* Undeclares everything the session still has. Holds the GVL (the lists
 * are only touched with it). */
static void zrb_session_detach_all(zrb_session *z) {
    while (z->qables != NULL) {
        zrb_qable_detach(z->qables); /* unlinks itself */
    }
    while (z->tokens != NULL) {
        zrb_token_detach(z->tokens);
    }
    while (z->subs != NULL) {
        zrb_sub_detach(z->subs);
    }
}

/* Closes the zenoh-c session. Without the GVL when called from Ruby
 * methods (it waits for zenoh's tasks), with it from the GC. */
static void *zrb_session_close_nogvl(void *p) {
    zrb_session *z = (zrb_session *)p;
    pthread_mutex_lock(&z->op_lock);
    if (z->live) {
        z->live = false;
        z_close(z_loan_mut(z->session), NULL);
        z_drop(z_move(z->session));
    }
    pthread_mutex_unlock(&z->op_lock);
    return NULL;
}

static void zrb_session_shutdown(zrb_session *z, bool with_gvl_release) {
    zrb_session_detach_all(z);
    if (!z->open) {
        return;
    }
    /* Closed for Ruby before the GVL is released: another Ruby thread that
     * runs while zenoh-c closes the session sees it closed and does not
     * touch the session any more (the calls without the GVL check live
     * under op_lock instead). */
    z->open = false;
    if (with_gvl_release) {
        rb_thread_call_without_gvl(zrb_session_close_nogvl, z, RUBY_UBF_IO, NULL);
    } else {
        zrb_session_close_nogvl(z);
    }
}

static void zrb_session_free(void *p) {
    zrb_session *z = (zrb_session *)p;
    zrb_session_shutdown(z, false);
    pthread_mutex_destroy(&z->op_lock);
    xfree(z);
}

static size_t zrb_session_size(const void *p) { return sizeof(zrb_session); }

static const rb_data_type_t zrb_session_type = {
    "Asterism::Zenoh::Session", {NULL, zrb_session_free, zrb_session_size}, NULL, NULL, 0};

static zrb_session *zrb_session_get(VALUE self) {
    zrb_session *z;
    TypedData_Get_Struct(self, zrb_session, &zrb_session_type, z);
    return z;
}

static void zrb_count_zid(const z_id_t *id, void *ctx) {
    (void)id;
    (*(int *)ctx)++;
}

/* Routers (client) or peers (peer mode) connected now. */
static int zrb_session_links(zrb_session *z) {
    int n = 0;
    z_owned_closure_zid_t cb;
    z_closure(&cb, zrb_count_zid, NULL, &n);
    if (z->peer) {
        z_info_peers_zid(z_loan(z->session), z_move(cb));
    } else {
        z_info_routers_zid(z_loan(z->session), z_move(cb));
    }
    return n;
}

/* Close the session when its connection can no longer carry it (see the
 * comment at the top). Returns true when the session is (now) closed. */
static bool zrb_session_check_link(zrb_session *z) {
    if (!z->open) {
        return true;
    }
    if (z_session_is_closed(z_loan(z->session))) {
        zrb_session_shutdown(z, true);
        return true;
    }
    if (z->peer && z->listening) {
        return false;
    }
    if (zrb_session_links(z) == 0) {
        zrb_session_shutdown(z, true);
        return true;
    }
    return false;
}

static zrb_session *zrb_session_get_open(VALUE self) {
    zrb_session *z = zrb_session_get(self);
    if (zrb_session_check_link(z)) {
        rb_raise(eZenohError, "session is closed");
    }
    return z;
}

/* A JSON5 string literal of s (for the configuration). */
static VALUE zrb_json_str(const char *s) {
    VALUE out = rb_str_new_cstr("\"");
    for (const char *p = s; *p; p++) {
        if (*p == '"' || *p == '\\') {
            rb_str_cat(out, "\\", 1);
        }
        rb_str_cat(out, p, 1);
    }
    rb_str_cat(out, "\"", 1);
    return out;
}

static bool zrb_config_set(z_owned_config_t *config, const char *key, const char *json5) {
    return zc_config_insert_json5(z_loan_mut(*config), key, json5) == Z_OK;
}

typedef struct {
    zrb_session *z;
    z_owned_config_t config;
    z_result_t ret;
} zrb_open_args;

static void *zrb_open_nogvl(void *p) {
    zrb_open_args *a = (zrb_open_args *)p;
    a->ret = z_open(&a->z->session, z_move(a->config), NULL);
    return NULL;
}

/* Asterism::Zenoh::Session.open(locator = nil, mode: :client, listen: nil) -> Session
 * - client: connects to the router at locator.
 * - peer: connects to the peer at locator (if given) and/or listens on
 *   listen (e.g. "tcp/0.0.0.0:7447"). At least one of them is needed.
 * Raises Asterism::Zenoh::Error when the session cannot be opened (no
 * router or peer answered within CONNECT_TIMEOUT_MS). */
static VALUE zrb_session_s_open(int argc, VALUE *argv, VALUE klass) {
    VALUE locator_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "01:", &locator_v, &opts);
    VALUE kw[2] = {Qundef, Qundef};
    if (!NIL_P(opts)) {
        ID ids[2] = {id_mode, id_listen};
        rb_get_kwargs(opts, ids, 0, 2, kw);
    }
    bool peer = false;
    if (kw[0] != Qundef && !NIL_P(kw[0])) {
        ID mode = zrb_sym_id(kw[0]);
        if (mode == id_peer) {
            peer = true;
        } else if (mode != id_client) {
            rb_raise(rb_eArgError, "mode must be :client or :peer");
        }
    }
    const char *locator = NIL_P(locator_v) ? NULL : StringValueCStr(locator_v);
    VALUE listen_v = (kw[1] == Qundef) ? Qnil : kw[1];
    const char *listen = NIL_P(listen_v) ? NULL : StringValueCStr(listen_v);
    if (!peer && listen != NULL) {
        rb_raise(rb_eArgError, "listen needs mode: :peer");
    }
    if (locator == NULL && listen == NULL) {
        rb_raise(rb_eArgError, "a locator (or, for a peer, listen:) is needed");
    }

    zrb_session *z;
    VALUE obj = TypedData_Make_Struct(klass, zrb_session, &zrb_session_type, z);
    pthread_mutex_init(&z->op_lock, NULL);

    /* The configuration mirrors the mruby gem's zenoh-pico build: no
     * scouting (the locator is given), a connect-only peer does not listen,
     * and the session fails to open when nobody answers in time. */
    zrb_open_args a;
    a.z = z;
    if (z_config_default(&a.config) != Z_OK) {
        rb_raise(eZenohError, "cannot create the configuration");
    }
    char num[32];
    snprintf(num, sizeof(num), "%d", ZRB_CONNECT_TIMEOUT_MS);
    bool ok = zrb_config_set(&a.config, "mode", peer ? "\"peer\"" : "\"client\"") &&
              zrb_config_set(&a.config, "scouting/multicast/enabled", "false") &&
              zrb_config_set(&a.config, "scouting/gossip/enabled", "false") &&
              zrb_config_set(&a.config, "connect/timeout_ms", num) &&
              zrb_config_set(&a.config, "connect/exit_on_failure", "true");
    if (ok) {
        snprintf(num, sizeof(num), "%d", ZRB_SEND_TIMEOUT_MS * 1000);
        ok = zrb_config_set(&a.config, "transport/link/tx/queue/congestion_control/block/wait_before_close", num);
    }
    if (ok && locator != NULL) {
        VALUE ep = rb_str_concat(rb_str_concat(rb_str_new_cstr("["), zrb_json_str(locator)), rb_str_new_cstr("]"));
        ok = zrb_config_set(&a.config, "connect/endpoints", StringValueCStr(ep));
    }
    if (ok) {
        if (listen != NULL) {
            VALUE ep = rb_str_concat(rb_str_concat(rb_str_new_cstr("["), zrb_json_str(listen)), rb_str_new_cstr("]"));
            ok = zrb_config_set(&a.config, "listen/endpoints", StringValueCStr(ep));
        } else {
            ok = zrb_config_set(&a.config, "listen/endpoints", "[]");
        }
    }
    if (!ok) {
        z_drop(z_move(a.config));
        rb_raise(eZenohError, "invalid locator");
    }
    rb_thread_call_without_gvl(zrb_open_nogvl, &a, RUBY_UBF_IO, NULL);
    if (a.ret != Z_OK) {
        rb_raise(eZenohError, "cannot open a session to %s (%d)", locator != NULL ? locator : listen, (int)a.ret);
    }
    z->open = true;
    z->live = true;
    z->peer = peer;
    z->listening = (listen != NULL);
    return obj;
}

typedef struct {
    zrb_session *z;
    z_owned_keyexpr_t key;
    z_owned_bytes_t payload;
    z_put_options_t opts;
    z_result_t ret;
} zrb_put_args;

static void *zrb_put_nogvl(void *p) {
    zrb_put_args *a = (zrb_put_args *)p;
    pthread_mutex_lock(&a->z->op_lock);
    if (a->z->live) {
        a->ret = z_put(z_loan(a->z->session), z_loan(a->key), z_move(a->payload), &a->opts);
    } else {
        a->ret = Z_ESESSION_CLOSED;
    }
    pthread_mutex_unlock(&a->z->op_lock);
    return NULL;
}

/* session.put(key, payload, attachment: nil) -> nil */
static VALUE zrb_session_put(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, payload, opts = Qnil;
    rb_scan_args(argc, argv, "2:", &key_v, &payload, &opts);
    VALUE kw[1] = {Qundef};
    if (!NIL_P(opts)) {
        rb_get_kwargs(opts, &id_attachment, 0, 1, kw);
    }
    const char *key = StringValueCStr(key_v);
    StringValue(payload);
    VALUE att = zrb_kw_attachment(kw[0]);
    zrb_session *z = zrb_session_get_open(self);

    zrb_put_args a;
    a.z = z;
    a.ret = Z_OK;
    zrb_owned_key(&a.key, key);
    if (z_bytes_copy_from_buf(&a.payload, (const uint8_t *)RSTRING_PTR(payload), (size_t)RSTRING_LEN(payload)) !=
        Z_OK) {
        z_drop(z_move(a.key));
        rb_raise(eZenohError, "cannot allocate the payload");
    }
    z_put_options_default(&a.opts);
    z_owned_bytes_t att_bytes;
    if (!NIL_P(att)) {
        if (z_bytes_copy_from_buf(&att_bytes, (const uint8_t *)RSTRING_PTR(att), (size_t)RSTRING_LEN(att)) != Z_OK) {
            z_drop(z_move(a.payload));
            z_drop(z_move(a.key));
            rb_raise(eZenohError, "cannot allocate the attachment");
        }
        a.opts.attachment = z_move(att_bytes);
    }
    rb_thread_call_without_gvl(zrb_put_nogvl, &a, RUBY_UBF_IO, NULL);
    z_drop(z_move(a.key));
    if (zrb_session_check_link(z)) {
        rb_raise(eZenohError, "put failed: the connection is lost (%d)", (int)a.ret);
    }
    if (a.ret != Z_OK) {
        rb_raise(eZenohError, "put failed (%d)", (int)a.ret);
    }
    return Qnil;
}

static VALUE zrb_sub_new(VALUE self, zrb_session *z, VALUE key_v, long depth, bool liveliness) {
    const char *key = StringValueCStr(key_v);
    z_view_keyexpr_t ke;
    zrb_view_key(&ke, key);

    zrb_sub *s;
    VALUE obj = TypedData_Make_Struct(liveliness ? cWatch : cSubscriber, zrb_sub, &zrb_sub_type, s);
    s->liveliness = liveliness;
    s->ch = zch_new(ZCH_SAMPLE, (uint32_t)depth);

    z_owned_closure_sample_t cb;
    zch_closure_sample(s->ch, &cb);
    z_result_t ret;
    if (liveliness) {
        /* history: the tokens alive now are reported first, as appearing. */
        z_liveliness_subscriber_options_t lo;
        z_liveliness_subscriber_options_default(&lo);
        lo.history = true;
        ret = z_liveliness_declare_subscriber(z_loan(z->session), &s->sub, z_loan(ke), z_move(cb), &lo);
    } else {
        /* Remote samples only: zenoh-pico (the mruby gem) does not deliver a
         * session's own puts to its own subscribers either. */
        z_subscriber_options_t so;
        z_subscriber_options_default(&so);
        so.allowed_origin = Z_LOCALITY_REMOTE;
        ret = z_declare_subscriber(z_loan(z->session), &s->sub, z_loan(ke), z_move(cb), &so);
    }
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot subscribe to %s (%d)", key, (int)ret);
    }
    s->declared = true;
    s->owner = z;
    s->next = z->subs;
    z->subs = s;
    zrb_hold_session(obj, self, key_v);
    return obj;
}

/* session.subscribe(key, depth = 16) -> Subscriber */
static VALUE zrb_session_subscribe(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, depth_v = Qnil;
    rb_scan_args(argc, argv, "11", &key_v, &depth_v);
    long depth = zrb_check_depth(depth_v);
    StringValueCStr(key_v);
    zrb_session *z = zrb_session_get_open(self);
    return zrb_sub_new(self, z, key_v, depth, false);
}

/* session.liveliness_watch(key, depth = 16) -> LivelinessWatch */
static VALUE zrb_session_liveliness_watch(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, depth_v = Qnil;
    rb_scan_args(argc, argv, "11", &key_v, &depth_v);
    long depth = zrb_check_depth(depth_v);
    StringValueCStr(key_v);
    zrb_session *z = zrb_session_get_open(self);
    return zrb_sub_new(self, z, key_v, depth, true);
}

/* session.queryable(key, depth = 16, complete: false) -> Queryable */
static VALUE zrb_session_queryable(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, depth_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "11:", &key_v, &depth_v, &opts);
    VALUE kw[1] = {Qundef};
    if (!NIL_P(opts)) {
        rb_get_kwargs(opts, &id_complete, 0, 1, kw);
    }
    bool complete = (kw[0] != Qundef) && RTEST(kw[0]);
    long depth = zrb_check_depth(depth_v);
    const char *key = StringValueCStr(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t ke;
    zrb_view_key(&ke, key);

    zrb_qable *q;
    VALUE obj = TypedData_Make_Struct(cQueryable, zrb_qable, &zrb_qable_type, q);
    q->ch = zch_new(ZCH_QUERY, (uint32_t)depth);
    z_owned_closure_query_t cb;
    zch_closure_query(q->ch, &cb);
    z_queryable_options_t qo;
    z_queryable_options_default(&qo);
    qo.complete = complete;
    /* Remote queries only, as with zenoh-pico (Asterism answers calls to
     * its own objects in place). */
    qo.allowed_origin = Z_LOCALITY_REMOTE;
    z_result_t ret = z_declare_queryable(z_loan(z->session), &q->qable, z_loan(ke), z_move(cb), &qo);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot declare a queryable on %s (%d)", key, (int)ret);
    }
    q->declared = true;
    q->owner = z;
    q->next = z->qables;
    z->qables = q;
    zrb_hold_session(obj, self, key_v);
    return obj;
}

static z_query_target_t zrb_get_target(VALUE v) {
    if (v == Qundef || NIL_P(v)) {
        return Z_QUERY_TARGET_ALL;
    }
    ID t = zrb_sym_id(v);
    if (t == id_all) {
        return Z_QUERY_TARGET_ALL;
    }
    if (t == id_all_complete) {
        return Z_QUERY_TARGET_ALL_COMPLETE;
    }
    if (t == id_best_matching) {
        return Z_QUERY_TARGET_BEST_MATCHING;
    }
    rb_raise(rb_eArgError, "target must be :all, :all_complete or :best_matching");
    return Z_QUERY_TARGET_ALL; /* not reached */
}

static z_query_consolidation_t zrb_get_consolidation(VALUE v) {
    if (v == Qundef || NIL_P(v)) {
        return z_query_consolidation_none();
    }
    ID c = zrb_sym_id(v);
    if (c == id_none) {
        return z_query_consolidation_none();
    }
    if (c == id_latest) {
        return z_query_consolidation_latest();
    }
    if (c == id_monotonic) {
        return z_query_consolidation_monotonic();
    }
    if (c == id_auto) {
        return z_query_consolidation_auto();
    }
    rb_raise(rb_eArgError, "consolidation must be :none, :latest, :monotonic or :auto");
    return z_query_consolidation_none(); /* not reached */
}

typedef struct {
    zrb_session *z;
    z_owned_keyexpr_t key;
    char *params; /* malloc'd copy, or NULL */
    bool liveliness;
    z_owned_closure_reply_t cb;
    z_get_options_t opts;
    z_liveliness_get_options_t lopts;
    z_result_t ret;
} zrb_get_args;

static void *zrb_get_nogvl(void *p) {
    zrb_get_args *a = (zrb_get_args *)p;
    pthread_mutex_lock(&a->z->op_lock);
    if (a->z->live) {
        if (a->liveliness) {
            a->ret = z_liveliness_get(z_loan(a->z->session), z_loan(a->key), z_move(a->cb), &a->lopts);
        } else {
            a->ret = z_get(z_loan(a->z->session), z_loan(a->key), a->params != NULL ? a->params : "", z_move(a->cb),
                           &a->opts);
        }
    } else {
        a->ret = Z_ESESSION_CLOSED;
    }
    pthread_mutex_unlock(&a->z->op_lock);
    /* A closure that was not taken (the session was closed) is dropped
     * here, which releases the channel's reference. */
    z_drop(z_move(a->cb));
    return NULL;
}

static VALUE zrb_run_get(VALUE self, zrb_session *z, zrb_get_args *a, VALUE obj, VALUE key_v, const char *what) {
    rb_thread_call_without_gvl(zrb_get_nogvl, a, RUBY_UBF_IO, NULL);
    z_drop(z_move(a->key));
    free(a->params);
    if (zrb_session_check_link(z)) {
        rb_raise(eZenohError, "%s failed: the connection is lost (%d)", what, (int)a->ret);
    }
    if (a->ret != Z_OK) {
        rb_raise(eZenohError, "%s failed (%d)", what, (int)a->ret);
    }
    zrb_hold_session(obj, self, key_v);
    return obj;
}

/* session.get(key, timeout_ms = 2000, params = nil, payload = nil,
 *             attachment: nil, target: :all, consolidation: :none) -> Get.
 * Returns at once; the replies come in on zenoh's threads. */
static VALUE zrb_session_get_m(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, timeout_v = Qnil, params_v = Qnil, payload = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "13:", &key_v, &timeout_v, &params_v, &payload, &opts);
    VALUE kw[3] = {Qundef, Qundef, Qundef};
    if (!NIL_P(opts)) {
        ID ids[3] = {id_attachment, id_target, id_consolidation};
        rb_get_kwargs(opts, ids, 0, 3, kw);
    }
    const char *key = StringValueCStr(key_v);
    long timeout_ms = zrb_check_timeout(timeout_v);
    const char *params = NIL_P(params_v) ? NULL : StringValueCStr(params_v);
    if (!NIL_P(payload) && !RB_TYPE_P(payload, T_STRING)) {
        rb_raise(rb_eTypeError, "payload must be a String");
    }
    VALUE att = zrb_kw_attachment(kw[0]);
    z_query_target_t target = zrb_get_target(kw[1]);
    z_query_consolidation_t consolidation = zrb_get_consolidation(kw[2]);
    zrb_session *z = zrb_session_get_open(self);

    zrb_get_args a;
    memset(&a, 0, sizeof(a));
    a.z = z;
    zrb_owned_key(&a.key, key);
    zrb_get *g;
    VALUE obj = zrb_get_new(&g);
    z_get_options_default(&a.opts);
    a.opts.timeout_ms = (uint64_t)timeout_ms;
    a.opts.target = target;
    a.opts.consolidation = consolidation;
    /* Remote queryables only, as with zenoh-pico. */
    a.opts.allowed_destination = Z_LOCALITY_REMOTE;
    z_owned_bytes_t bytes;
    z_owned_bytes_t att_bytes;
    if (!NIL_P(payload)) {
        zrb_bytes_from_str(&bytes, payload, "payload");
        a.opts.payload = z_move(bytes);
    }
    if (!NIL_P(att)) {
        zrb_bytes_from_str(&att_bytes, att, "attachment");
        a.opts.attachment = z_move(att_bytes);
    }
    if (params != NULL) {
        a.params = strdup(params);
    }
    zch_closure_reply(g->ch, &a.cb);
    return zrb_run_get(self, z, &a, obj, key_v, "get");
}

/* session.liveliness(key) -> LivelinessToken */
static VALUE zrb_session_liveliness(VALUE self, VALUE key_v) {
    const char *key = StringValueCStr(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t ke;
    zrb_view_key(&ke, key);
    zrb_token *t;
    VALUE obj = TypedData_Make_Struct(cToken, zrb_token, &zrb_token_type, t);
    z_result_t ret = z_liveliness_declare_token(z_loan(z->session), &t->token, z_loan(ke), NULL);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot declare a liveliness token on %s (%d)", key, (int)ret);
    }
    t->declared = true;
    t->owner = z;
    t->next = z->tokens;
    z->tokens = t;
    zrb_hold_session(obj, self, key_v);
    return obj;
}

/* session.liveliness_get(key, timeout_ms = 2000) -> Get */
static VALUE zrb_session_liveliness_get(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, timeout_v = Qnil;
    rb_scan_args(argc, argv, "11", &key_v, &timeout_v);
    const char *key = StringValueCStr(key_v);
    long timeout_ms = zrb_check_timeout(timeout_v);
    zrb_session *z = zrb_session_get_open(self);

    zrb_get_args a;
    memset(&a, 0, sizeof(a));
    a.z = z;
    a.liveliness = true;
    zrb_owned_key(&a.key, key);
    zrb_get *g;
    VALUE obj = zrb_get_new(&g);
    z_liveliness_get_options_default(&a.lopts);
    a.lopts.timeout_ms = (uint64_t)timeout_ms;
    zch_closure_reply(g->ch, &a.cb);
    return zrb_run_get(self, z, &a, obj, key_v, "liveliness_get");
}

/* session.poll(steps = 8) -> true while the session is open, false once
 * closed (by close, or because the connection was lost). zenoh-c receives
 * on its own threads, so this only checks the connection; steps is
 * accepted for the mruby gem's signature. Never waits. */
static VALUE zrb_session_poll(int argc, VALUE *argv, VALUE self) {
    VALUE steps_v = Qnil;
    rb_scan_args(argc, argv, "01", &steps_v);
    if (!NIL_P(steps_v)) {
        NUM2LONG(steps_v);
    }
    return zrb_session_check_link(zrb_session_get(self)) ? Qfalse : Qtrue;
}

static VALUE zrb_session_closed_p(VALUE self) {
    return zrb_session_check_link(zrb_session_get(self)) ? Qtrue : Qfalse;
}

static VALUE zrb_session_close(VALUE self) {
    zrb_session_shutdown(zrb_session_get(self), true);
    return Qnil;
}

/* session.zid -> String: this session's Zenoh ID in hex. */
static VALUE zrb_session_zid(VALUE self) {
    zrb_session *z = zrb_session_get_open(self);
    z_id_t id = z_info_zid(z_loan(z->session));
    z_owned_string_t str;
    z_id_to_string(&id, &str);
    VALUE out = zrb_str_from_view(z_loan(str));
    z_drop(z_move(str));
    return out;
}

/* session.peers -> Integer: connected peers (peer mode), or routers (1
 * normally) for a client session; 0 once closed. */
static VALUE zrb_session_peers(VALUE self) {
    zrb_session *z = zrb_session_get(self);
    if (zrb_session_check_link(z)) {
        return INT2FIX(0);
    }
    return INT2FIX(zrb_session_links(z));
}

/* The default of zenoh-c's transport/unicast/max_sessions (MAX_PEERS). */
static long zrb_default_max_sessions(void) {
    long n = -1;
    z_owned_config_t c;
    if (z_config_default(&c) != Z_OK) {
        return n;
    }
    z_owned_string_t s;
    if (zc_config_get_from_str(z_loan(c), "transport/unicast/max_sessions", &s) == Z_OK) {
        char buf[32];
        size_t len = z_string_len(z_loan(s));
        if (len < sizeof(buf)) {
            memcpy(buf, z_string_data(z_loan(s)), len);
            buf[len] = '\0';
            n = strtol(buf, NULL, 10);
        }
        z_drop(z_move(s));
    }
    z_drop(z_move(c));
    return n;
}

/* ----------------------------------------------------------------- init */

void Init_asterism_zenoh(void) {
    /* Asterism::Zenoh. No top-level Zenoh (require "asterism/zenoh/global"). */
    mAsterism = rb_define_module("Asterism");
    mZenoh = rb_define_module_under(mAsterism, "Zenoh");
    eZenohError = rb_define_class_under(mZenoh, "Error", rb_eStandardError);
    rb_define_const(mZenoh, "C_VERSION", rb_str_freeze(rb_str_new_cstr(ZENOH_C)));
    rb_define_const(mZenoh, "CONNECT_TIMEOUT_MS", INT2FIX(ZRB_CONNECT_TIMEOUT_MS));
    rb_define_const(mZenoh, "SEND_TIMEOUT_MS", INT2FIX(ZRB_SEND_TIMEOUT_MS));
    rb_define_const(mZenoh, "PEER", Qtrue);
    rb_define_const(mZenoh, "MAX_PEERS", LONG2NUM(zrb_default_max_sessions()));

    id_attachment = rb_intern("attachment");
    id_target = rb_intern("target");
    id_consolidation = rb_intern("consolidation");
    id_complete = rb_intern("complete");
    id_mode = rb_intern("mode");
    id_listen = rb_intern("listen");
    id_iv_session = rb_intern("@session");
    id_iv_key = rb_intern("@key");
    id_client = rb_intern("client");
    id_peer = rb_intern("peer");
    id_all = rb_intern("all");
    id_all_complete = rb_intern("all_complete");
    id_best_matching = rb_intern("best_matching");
    id_none = rb_intern("none");
    id_latest = rb_intern("latest");
    id_monotonic = rb_intern("monotonic");
    id_auto = rb_intern("auto");

    cSession = rb_define_class_under(mZenoh, "Session", rb_cObject);
    zrb_no_new(cSession);
    rb_define_singleton_method(cSession, "open", zrb_session_s_open, -1);
    rb_define_method(cSession, "put", zrb_session_put, -1);
    rb_define_method(cSession, "subscribe", zrb_session_subscribe, -1);
    rb_define_method(cSession, "get", zrb_session_get_m, -1);
    rb_define_method(cSession, "queryable", zrb_session_queryable, -1);
    rb_define_method(cSession, "liveliness", zrb_session_liveliness, 1);
    rb_define_method(cSession, "liveliness_watch", zrb_session_liveliness_watch, -1);
    rb_define_method(cSession, "liveliness_get", zrb_session_liveliness_get, -1);
    rb_define_method(cSession, "poll", zrb_session_poll, -1);
    rb_define_method(cSession, "peers", zrb_session_peers, 0);
    rb_define_method(cSession, "zid", zrb_session_zid, 0);
    rb_define_method(cSession, "closed?", zrb_session_closed_p, 0);
    rb_define_method(cSession, "close", zrb_session_close, 0);

    /* Subscriber and LivelinessWatch share one C structure. */
    cSubscriber = rb_define_class_under(mZenoh, "Subscriber", rb_cObject);
    cWatch = rb_define_class_under(mZenoh, "LivelinessWatch", rb_cObject);
    VALUE both[2] = {cSubscriber, cWatch};
    for (int i = 0; i < 2; i++) {
        zrb_no_new(both[i]);
        rb_define_method(both[i], "each_pending", zrb_sub_each_pending, 0);
        rb_define_method(both[i], "pending", zrb_sub_pending, 0);
        rb_define_method(both[i], "received", zrb_sub_received, 0);
        rb_define_method(both[i], "dropped", zrb_sub_dropped, 0);
        rb_define_method(both[i], "close", zrb_sub_close, 0);
        rb_define_method(both[i], "closed?", zrb_sub_closed_p, 0);
    }

    cQueryable = rb_define_class_under(mZenoh, "Queryable", rb_cObject);
    zrb_no_new(cQueryable);
    rb_define_method(cQueryable, "each_pending", zrb_qable_each_pending, 0);
    rb_define_method(cQueryable, "pending", zrb_qable_pending, 0);
    rb_define_method(cQueryable, "received", zrb_qable_received, 0);
    rb_define_method(cQueryable, "dropped", zrb_qable_dropped, 0);
    rb_define_method(cQueryable, "close", zrb_qable_close, 0);
    rb_define_method(cQueryable, "closed?", zrb_qable_closed_p, 0);

    cQuery = rb_define_class_under(mZenoh, "Query", rb_cObject);
    zrb_no_new(cQuery);
    rb_define_method(cQuery, "key", zrb_query_key, 0);
    rb_define_method(cQuery, "params", zrb_query_params, 0);
    rb_define_method(cQuery, "payload", zrb_query_payload, 0);
    rb_define_method(cQuery, "attachment", zrb_query_attachment, 0);
    rb_define_method(cQuery, "reply", zrb_query_reply, -1);
    rb_define_method(cQuery, "finish", zrb_query_finish_m, 0);
    rb_define_method(cQuery, "finished?", zrb_query_finished_p, 0);

    cGet = rb_define_class_under(mZenoh, "Get", rb_cObject);
    zrb_no_new(cGet);
    rb_define_method(cGet, "each_reply", zrb_get_each_reply, 0);
    rb_define_method(cGet, "done?", zrb_get_done_p, 0);
    rb_define_method(cGet, "pending", zrb_get_pending, 0);
    rb_define_method(cGet, "received", zrb_get_received, 0);
    rb_define_method(cGet, "dropped", zrb_get_dropped, 0);
    rb_define_method(cGet, "errors", zrb_get_errors, 0);

    cToken = rb_define_class_under(mZenoh, "LivelinessToken", rb_cObject);
    zrb_no_new(cToken);
    rb_define_method(cToken, "close", zrb_token_close, 0);
    rb_define_method(cToken, "closed?", zrb_token_closed_p, 0);
}
