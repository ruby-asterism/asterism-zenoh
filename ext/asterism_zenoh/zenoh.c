/*
 * asterism-zenoh: Asterism::Zenoh for CRuby, over zenoh-c.
 *
 * The Ruby API is the one of the mruby / PicoRuby gem
 * (ruby-asterism/picoruby-asterism-zenoh, over zenoh-pico): sessions
 * (client, or peer with an optional listener), put / subscribe, get /
 * queryable (query and reply), liveliness tokens and watches, attachments,
 * Session#zid. Code written for one runs on the other. 0.3.0 adds, for
 * CRuby only, the configuration, scouting, delete, declared publishers and
 * queriers, matching status, the fields of samples and replies, error and
 * delete replies, encodings, timestamps, key expressions, the advanced
 * publisher / subscriber, transport and link events and the log
 * (docs/feature_coverage.md). They receive in the same way: what zenoh-c's
 * callbacks get is copied into a queue (a channel, or a zevq for small
 * events) and Ruby takes it out by polling.
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
static VALUE cAdvSubscriber, cPublisher, cAdvPublisher, cQuerier, cKeyExpr, cTimestamp;
static VALUE cMatchingListener, cEventListener;
static ID id_attachment, id_target, id_consolidation, id_complete, id_mode, id_listen;
static ID id_config, id_config_file, id_scouting, id_history, id_new;
static ID id_iv_session, id_iv_key, id_iv_parent;
static ID id_client, id_peer, id_router, id_all, id_all_complete, id_best_matching;
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

/* Error replies are queued too (each_result returns them; each_reply skips
 * them). They are counted in errors, the others in received. */
static void zch_on_reply(z_loaned_reply_t *reply, void *ctx) {
    zch *c = (zch *)ctx;
    bool ok = z_reply_is_ok(reply);
    pthread_mutex_lock(&c->lock);
    zch_make_room(c);
    z_closure_reply_call(z_loan(c->tx.reply), reply);
    atomic_fetch_add(&c->queued, 1);
    atomic_fetch_add(ok ? &c->received : &c->errors, 1);
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

/* zenoh-c's text for the last error on this thread (": ..."), or "". Read
 * right after the failing call: the next call that can fail replaces it. */
static VALUE zrb_last_error(void) {
    z_view_string_t s;
    zc_get_last_error(&s);
    size_t n = z_string_len(z_loan(s));
    if (n == 0) {
        return rb_str_new_cstr("");
    }
    return rb_sprintf(": %.*s", (int)n, z_string_data(z_loan(s)));
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
typedef struct zrb_ent zrb_ent;

/* The entities added in 0.3.0 (publishers, queriers, listeners, declared
 * key expressions) share one list of the session. undeclare drops the
 * zenoh-c object; order says when the session undeclares it on close
 * (listeners first, declared key expressions last). */
enum { ZENT_LISTENER = 0, ZENT_ENTITY = 1, ZENT_KEYEXPR = 2 };

struct zrb_ent {
    zrb_session *owner; /* NULL once detached */
    zrb_ent *next;
    int order;
    bool declared;
    bool gvl_free_use; /* used without the GVL: undeclared under op_lock */
    void (*undeclare)(zrb_ent *e);
};

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
    zrb_ent *ents;  /* the rest (0.3.0) */
};

typedef enum { ZSUB_PLAIN, ZSUB_LIVELINESS, ZSUB_ADVANCED } zsub_type;

struct zrb_sub {
    union {
        z_owned_subscriber_t sub; /* plain and liveliness */
        ze_owned_advanced_subscriber_t adv;
    } u;
    bool declared;
    bool liveliness;
    zsub_type type;
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

/* ----------------------------------------------- entities (0.3.0 list) */

static void zrb_ent_link(zrb_ent *e, zrb_session *z, int order, bool gvl_free_use, void (*undeclare)(zrb_ent *)) {
    e->owner = z;
    e->order = order;
    e->declared = true;
    e->gvl_free_use = gvl_free_use;
    e->undeclare = undeclare;
    e->next = z->ents;
    z->ents = e;
}

/* Undeclares the entity (once) and takes it off its session's list. With
 * the GVL held. An entity that other threads use without the GVL
 * (publishers, queriers) is undeclared under the session's op_lock, so a
 * put in progress finishes first. */
static void zrb_ent_detach(zrb_ent *e) {
    if (e->declared) {
        zrb_session *z = e->owner;
        bool lock = e->gvl_free_use && z != NULL;
        if (lock) {
            pthread_mutex_lock(&z->op_lock);
        }
        e->undeclare(e);
        e->declared = false;
        if (lock) {
            pthread_mutex_unlock(&z->op_lock);
        }
    }
    if (e->owner != NULL) {
        zrb_ent **pp = &e->owner->ents;
        while (*pp != NULL) {
            if (*pp == e) {
                *pp = e->next;
                break;
            }
            pp = &(*pp)->next;
        }
        e->owner = NULL;
        e->next = NULL;
    }
}

/* -------------------------------------------------------- event queues */

/* Small events (matching status, transports and links, missed samples,
 * scouting answers) copied out of zenoh-c's callbacks into plain C memory.
 * Same rules as the channels: the callbacks never touch Ruby, a full queue
 * drops its oldest entry, Ruby takes the entries out by polling. */
typedef enum { ZEV_MATCHING, ZEV_TRANSPORT, ZEV_LINK, ZEV_MISS, ZEV_HELLO } zev_kind;

typedef struct zev {
    struct zev *next;
    bool flag; /* matching status; added (not removed) for transports / links */
    z_id_t zid;
    uint32_t eid, nb;
    int whatami;
    bool qos, multicast, streamed, has_reliability;
    int reliability;
    uint16_t mtu;
    char *src, *dst, *group, *auth;
    char **strs; /* interfaces of a link, locators of a hello */
    size_t nstrs;
} zev;

typedef struct {
    zev_kind kind;
    pthread_mutex_t lock;
    zev *head, *tail;
    uint32_t depth;
    atomic_uint queued;
    atomic_uint received;
    atomic_uint dropped;
    atomic_bool done;
    atomic_int refs;
} zevq;

static void zev_free(zev *e) {
    if (e == NULL) {
        return;
    }
    free(e->src);
    free(e->dst);
    free(e->group);
    free(e->auth);
    for (size_t i = 0; i < e->nstrs; i++) {
        free(e->strs[i]);
    }
    free(e->strs);
    free(e);
}

static zevq *zevq_new(zev_kind kind, uint32_t depth) {
    zevq *q = (zevq *)calloc(1, sizeof(zevq));
    if (q == NULL) {
        rb_raise(rb_eNoMemError, "cannot allocate an event queue");
    }
    q->kind = kind;
    q->depth = depth;
    pthread_mutex_init(&q->lock, NULL);
    atomic_init(&q->queued, 0);
    atomic_init(&q->received, 0);
    atomic_init(&q->dropped, 0);
    atomic_init(&q->done, false);
    atomic_init(&q->refs, 1);
    return q;
}

static void zevq_unref(zevq *q) {
    if (q == NULL || atomic_fetch_sub(&q->refs, 1) != 1) {
        return;
    }
    zev *e = q->head;
    while (e != NULL) {
        zev *n = e->next;
        zev_free(e);
        e = n;
    }
    pthread_mutex_destroy(&q->lock);
    free(q);
}

/* The context of a closure handed to zenoh-c holds a reference. */
static zevq *zevq_ref(zevq *q) {
    atomic_fetch_add(&q->refs, 1);
    return q;
}

static void zevq_on_drop(void *ctx) {
    zevq *q = (zevq *)ctx;
    atomic_store(&q->done, true);
    zevq_unref(q);
}

/* From zenoh's threads (or, for the snapshots, the calling one). */
static void zevq_push(zevq *q, zev *e) {
    if (e == NULL) {
        return;
    }
    pthread_mutex_lock(&q->lock);
    if (atomic_load(&q->queued) >= q->depth && q->head != NULL) {
        zev *old = q->head;
        q->head = old->next;
        if (q->head == NULL) {
            q->tail = NULL;
        }
        zev_free(old);
        atomic_fetch_sub(&q->queued, 1);
        atomic_fetch_add(&q->dropped, 1);
    }
    e->next = NULL;
    if (q->tail != NULL) {
        q->tail->next = e;
    } else {
        q->head = e;
    }
    q->tail = e;
    atomic_fetch_add(&q->queued, 1);
    atomic_fetch_add(&q->received, 1);
    pthread_mutex_unlock(&q->lock);
}

static zev *zevq_take(zevq *q) {
    pthread_mutex_lock(&q->lock);
    zev *e = q->head;
    if (e != NULL) {
        q->head = e->next;
        if (q->head == NULL) {
            q->tail = NULL;
        }
        atomic_fetch_sub(&q->queued, 1);
    }
    pthread_mutex_unlock(&q->lock);
    return e;
}

static char *zev_strdup_n(const char *s, size_t n) {
    char *out = (char *)malloc(n + 1);
    if (out != NULL) {
        memcpy(out, s, n);
        out[n] = '\0';
    }
    return out;
}

/* Copies an owned string out (and drops it). */
static char *zev_take_string(z_owned_string_t *s) {
    char *out = zev_strdup_n(z_string_data(z_loan(*s)), z_string_len(z_loan(*s)));
    z_drop(z_move(*s));
    return out;
}

static void zev_take_strings(zev *e, z_owned_string_array_t *arr) {
    size_t n = z_string_array_len(z_loan(*arr));
    e->strs = (char **)calloc(n > 0 ? n : 1, sizeof(char *));
    if (e->strs != NULL) {
        for (size_t i = 0; i < n; i++) {
            const z_loaned_string_t *s = z_string_array_get(z_loan(*arr), i);
            e->strs[i] = zev_strdup_n(z_string_data(s), z_string_len(s));
        }
        e->nstrs = n;
    }
    z_drop(z_move(*arr));
}

static zev *zev_new(void) { return (zev *)calloc(1, sizeof(zev)); }

static void zev_fill_transport(zev *e, const z_loaned_transport_t *t) {
    e->zid = z_transport_zid(t);
    e->whatami = (int)z_transport_whatami(t);
    e->qos = z_transport_is_qos(t);
    e->multicast = z_transport_is_multicast(t);
}

static void zev_fill_link(zev *e, const z_loaned_link_t *l) {
    z_owned_string_t s;
    e->zid = z_link_zid(l);
    z_link_src(l, &s);
    e->src = zev_take_string(&s);
    z_link_dst(l, &s);
    e->dst = zev_take_string(&s);
    z_link_group(l, &s);
    e->group = zev_take_string(&s);
    z_link_auth_identifier(l, &s);
    e->auth = zev_take_string(&s);
    e->mtu = z_link_mtu(l);
    e->streamed = z_link_is_streamed(l);
    z_reliability_t rel;
    e->has_reliability = z_link_reliability(l, &rel);
    e->reliability = (int)rel;
    z_owned_string_array_t arr;
    z_link_interfaces(l, &arr);
    zev_take_strings(e, &arr);
}

/* The callbacks (zenoh's threads; no Ruby). */
static void zev_on_matching(const z_matching_status_t *st, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->flag = st->matching;
    }
    zevq_push((zevq *)ctx, e);
}

static void zev_on_transport_event(z_loaned_transport_event_t *ev, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->flag = (z_transport_event_kind(ev) == Z_SAMPLE_KIND_PUT);
        zev_fill_transport(e, z_transport_event_transport(ev));
    }
    zevq_push((zevq *)ctx, e);
}

static void zev_on_transport(z_loaned_transport_t *t, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->flag = true;
        zev_fill_transport(e, t);
    }
    zevq_push((zevq *)ctx, e);
}

static void zev_on_link_event(z_loaned_link_event_t *ev, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->flag = (z_link_event_kind(ev) == Z_SAMPLE_KIND_PUT);
        zev_fill_link(e, z_link_event_link(ev));
    }
    zevq_push((zevq *)ctx, e);
}

static void zev_on_link(z_loaned_link_t *l, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->flag = true;
        zev_fill_link(e, l);
    }
    zevq_push((zevq *)ctx, e);
}

static void zev_on_miss(const ze_miss_t *m, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->zid = z_entity_global_id_zid(&m->source);
        e->eid = z_entity_global_id_eid(&m->source);
        e->nb = m->nb;
    }
    zevq_push((zevq *)ctx, e);
}

static void zev_on_hello(z_loaned_hello_t *h, void *ctx) {
    zev *e = zev_new();
    if (e != NULL) {
        e->zid = z_hello_zid(h);
        e->whatami = (int)z_hello_whatami(h);
        z_owned_string_array_t arr;
        z_hello_locators(h, &arr);
        zev_take_strings(e, &arr);
    }
    zevq_push((zevq *)ctx, e);
}

/* --------------------------------------------------- values for Ruby */

/* The value classes are Ruby (lib/asterism/zenoh/values.rb). */
static VALUE zrb_value_class(const char *name) { return rb_const_get(mZenoh, rb_intern(name)); }

static VALUE zrb_new_value(const char *klass, VALUE hash) {
    return rb_class_new_instance_kw(1, &hash, zrb_value_class(klass), RB_PASS_KEYWORDS);
}

static void zrb_hset(VALUE h, const char *key, VALUE v) { rb_hash_aset(h, ID2SYM(rb_intern(key)), v); }

static VALUE zrb_zid_value(const z_id_t *id) {
    z_owned_string_t str;
    z_id_to_string(id, &str);
    VALUE out = zrb_str_from_view(z_loan(str));
    z_drop(z_move(str));
    return out;
}

static VALUE zrb_cstr_or_nil(const char *s) { return s == NULL ? Qnil : rb_utf8_str_new_cstr(s); }

static VALUE zrb_whatami_sym(int w) {
    switch (w) {
    case Z_WHATAMI_ROUTER:
        return ID2SYM(rb_intern("router"));
    case Z_WHATAMI_PEER:
        return ID2SYM(rb_intern("peer"));
    case Z_WHATAMI_CLIENT:
        return ID2SYM(rb_intern("client"));
    }
    return Qnil;
}

static VALUE zrb_reliability_sym(int r) {
    return ID2SYM(rb_intern(r == Z_RELIABILITY_BEST_EFFORT ? "best_effort" : "reliable"));
}

static VALUE zrb_strs_value(const zev *e) {
    VALUE a = rb_ary_new_capa((long)e->nstrs);
    for (size_t i = 0; i < e->nstrs; i++) {
        rb_ary_push(a, zrb_cstr_or_nil(e->strs[i]));
    }
    return a;
}

/* event: true for a listener (adds kind), false for a snapshot. */
static VALUE zev_to_ruby(zev_kind kind, const zev *e, bool event) {
    VALUE h = rb_hash_new();
    switch (kind) {
    case ZEV_MATCHING:
        return e->flag ? Qtrue : Qfalse;
    case ZEV_TRANSPORT:
        if (event) {
            zrb_hset(h, "kind", ID2SYM(rb_intern(e->flag ? "added" : "removed")));
        }
        zrb_hset(h, "zid", zrb_zid_value(&e->zid));
        zrb_hset(h, "whatami", zrb_whatami_sym(e->whatami));
        zrb_hset(h, "qos", e->qos ? Qtrue : Qfalse);
        zrb_hset(h, "multicast", e->multicast ? Qtrue : Qfalse);
        return zrb_new_value(event ? "TransportEvent" : "Transport", h);
    case ZEV_LINK:
        if (event) {
            zrb_hset(h, "kind", ID2SYM(rb_intern(e->flag ? "added" : "removed")));
        }
        zrb_hset(h, "zid", zrb_zid_value(&e->zid));
        zrb_hset(h, "src", zrb_cstr_or_nil(e->src));
        zrb_hset(h, "dst", zrb_cstr_or_nil(e->dst));
        zrb_hset(h, "mtu", UINT2NUM(e->mtu));
        zrb_hset(h, "streamed", e->streamed ? Qtrue : Qfalse);
        zrb_hset(h, "reliability", e->has_reliability ? zrb_reliability_sym(e->reliability) : Qnil);
        zrb_hset(h, "interfaces", zrb_strs_value(e));
        zrb_hset(h, "group", (e->group && *e->group) ? zrb_cstr_or_nil(e->group) : Qnil);
        zrb_hset(h, "auth_identifier", (e->auth && *e->auth) ? zrb_cstr_or_nil(e->auth) : Qnil);
        return zrb_new_value(event ? "LinkEvent" : "Link", h);
    case ZEV_MISS:
        zrb_hset(h, "source_zid", zrb_zid_value(&e->zid));
        zrb_hset(h, "source_eid", UINT2NUM(e->eid));
        zrb_hset(h, "count", UINT2NUM(e->nb));
        return zrb_new_value("Miss", h);
    case ZEV_HELLO:
        zrb_hset(h, "zid", zrb_zid_value(&e->zid));
        zrb_hset(h, "whatami", zrb_whatami_sym(e->whatami));
        zrb_hset(h, "locators", zrb_strs_value(e));
        return zrb_new_value("Hello", h);
    }
    return Qnil;
}

/* Takes what is queued now, oldest first; yields each, or returns them. */
static VALUE zevq_each_pending(zevq *q, bool event) {
    bool collect = !rb_block_given_p();
    VALUE out = collect ? rb_ary_new() : Qnil;
    long taken = 0;
    unsigned todo = atomic_load(&q->queued);
    while (todo > 0) {
        todo--;
        zev *e = zevq_take(q);
        if (e == NULL) {
            break;
        }
        VALUE v = zev_to_ruby(q->kind, e, event);
        zev_free(e);
        taken++;
        if (collect) {
            rb_ary_push(out, v);
        } else {
            rb_yield(v);
        }
    }
    return collect ? out : LONG2NUM(taken);
}

/* ------------------------------------------------------------ options */

/* Keyword arguments of the calls that send (put, delete, publishers,
 * replies, queriers). -1: not given (zenoh-c's default). */
typedef struct {
    VALUE attachment; /* nil or String */
    VALUE encoding;   /* nil or String */
    VALUE timestamp;  /* nil, true or a Timestamp */
    VALUE payload;    /* nil or String */
    int priority;
    int congestion_control;
    int express;
    int reliability;
    int destination;
    int accept_replies;
    VALUE target, consolidation, timeout;
    bool has_target, has_consolidation, has_timeout;
} zrb_opts;

static int zrb_priority_arg(VALUE v) {
    if (RB_INTEGER_TYPE_P(v)) {
        int p = NUM2INT(v);
        if (p < Z_PRIORITY_REAL_TIME || p > Z_PRIORITY_BACKGROUND) {
            rb_raise(rb_eArgError, "priority must be 1..7");
        }
        return p;
    }
    static const char *const names[] = {"real_time", "interactive_high", "interactive_low", "data_high",
                                        "data", "data_low", "background"};
    ID id = zrb_sym_id(v);
    for (int i = 0; i < 7; i++) {
        if (id == rb_intern(names[i])) {
            return i + 1;
        }
    }
    rb_raise(rb_eArgError,
             "priority must be :real_time, :interactive_high, :interactive_low, :data_high, :data, :data_low, "
             ":background or 1..7");
    return 0;
}

static VALUE zrb_priority_sym(int p) {
    static const char *const names[] = {"real_time", "interactive_high", "interactive_low", "data_high",
                                        "data", "data_low", "background"};
    if (p < 1 || p > 7) {
        return Qnil;
    }
    return ID2SYM(rb_intern(names[p - 1]));
}

static int zrb_cc_arg(VALUE v) {
    ID id = zrb_sym_id(v);
    if (id == rb_intern("drop")) {
        return Z_CONGESTION_CONTROL_DROP;
    }
    if (id == rb_intern("block")) {
        return Z_CONGESTION_CONTROL_BLOCK;
    }
    if (id == rb_intern("block_first")) {
        return Z_CONGESTION_CONTROL_BLOCK_FIRST;
    }
    rb_raise(rb_eArgError, "congestion_control must be :drop, :block or :block_first");
    return 0;
}

static VALUE zrb_cc_sym(int c) {
    switch (c) {
    case Z_CONGESTION_CONTROL_BLOCK:
        return ID2SYM(rb_intern("block"));
    case Z_CONGESTION_CONTROL_DROP:
        return ID2SYM(rb_intern("drop"));
    case Z_CONGESTION_CONTROL_BLOCK_FIRST:
        return ID2SYM(rb_intern("block_first"));
    }
    return Qnil;
}

static int zrb_reliability_arg(VALUE v) {
    ID id = zrb_sym_id(v);
    if (id == rb_intern("reliable")) {
        return Z_RELIABILITY_RELIABLE;
    }
    if (id == rb_intern("best_effort")) {
        return Z_RELIABILITY_BEST_EFFORT;
    }
    rb_raise(rb_eArgError, "reliability must be :reliable or :best_effort");
    return 0;
}

static int zrb_locality_arg(VALUE v) {
    ID id = zrb_sym_id(v);
    if (id == rb_intern("any")) {
        return Z_LOCALITY_ANY;
    }
    if (id == rb_intern("remote")) {
        return Z_LOCALITY_REMOTE;
    }
    if (id == rb_intern("session_local")) {
        return Z_LOCALITY_SESSION_LOCAL;
    }
    rb_raise(rb_eArgError, "allowed_destination must be :any, :remote or :session_local");
    return 0;
}

static VALUE zrb_str_or_nil_arg(VALUE v, const char *what) {
    if (v == Qundef || NIL_P(v)) {
        return Qnil;
    }
    if (!RB_TYPE_P(v, T_STRING)) {
        rb_raise(rb_eTypeError, "%s must be a String or nil", what);
    }
    return v;
}

static bool zrb_is_timestamp(VALUE v);

/* names: the keywords the call accepts (unknown ones raise ArgumentError). */
static void zrb_opts_parse(VALUE hash, const char *const *names, int n, zrb_opts *o) {
    o->attachment = o->encoding = o->timestamp = o->payload = Qnil;
    o->priority = o->congestion_control = o->express = o->reliability = o->destination = o->accept_replies = -1;
    o->target = o->consolidation = o->timeout = Qnil;
    o->has_target = o->has_consolidation = o->has_timeout = false;
    if (NIL_P(hash)) {
        return;
    }
    ID ids[16];
    VALUE kw[16];
    for (int i = 0; i < n; i++) {
        ids[i] = rb_intern(names[i]);
    }
    rb_get_kwargs(hash, ids, 0, n, kw);
    for (int i = 0; i < n; i++) {
        VALUE v = kw[i];
        if (v == Qundef) {
            continue;
        }
        const char *k = names[i];
        if (strcmp(k, "attachment") == 0) {
            o->attachment = zrb_kw_attachment(v);
        } else if (strcmp(k, "payload") == 0) {
            o->payload = zrb_str_or_nil_arg(v, "payload");
        } else if (strcmp(k, "encoding") == 0) {
            if (!NIL_P(v)) {
                o->encoding = rb_String(v);
            }
        } else if (strcmp(k, "timestamp") == 0) {
            if (NIL_P(v) || v == Qfalse) {
                o->timestamp = Qnil;
            } else if (v == Qtrue || zrb_is_timestamp(v)) {
                o->timestamp = v;
            } else {
                rb_raise(rb_eTypeError, "timestamp must be true, nil or an Asterism::Zenoh::Timestamp");
            }
        } else if (NIL_P(v)) {
            /* nil: zenoh-c's default */
        } else if (strcmp(k, "priority") == 0) {
            o->priority = zrb_priority_arg(v);
        } else if (strcmp(k, "congestion_control") == 0) {
            o->congestion_control = zrb_cc_arg(v);
        } else if (strcmp(k, "express") == 0) {
            o->express = RTEST(v) ? 1 : 0;
        } else if (strcmp(k, "reliability") == 0) {
            o->reliability = zrb_reliability_arg(v);
        } else if (strcmp(k, "allowed_destination") == 0) {
            o->destination = zrb_locality_arg(v);
        } else if (strcmp(k, "accept_replies") == 0) {
            ID id = zrb_sym_id(v);
            if (id == rb_intern("any")) {
                o->accept_replies = Z_REPLY_KEYEXPR_ANY;
            } else if (id == rb_intern("matching_query")) {
                o->accept_replies = Z_REPLY_KEYEXPR_MATCHING_QUERY;
            } else {
                rb_raise(rb_eArgError, "accept_replies must be :matching_query or :any");
            }
        } else if (strcmp(k, "target") == 0) {
            o->target = v;
            o->has_target = true;
        } else if (strcmp(k, "consolidation") == 0) {
            o->consolidation = v;
            o->has_consolidation = true;
        } else if (strcmp(k, "timeout_ms") == 0) {
            o->timeout = v;
            o->has_timeout = true;
        }
    }
}

/* ----------------------------------------------------------- encodings */

static VALUE zrb_encoding_value(const z_loaned_encoding_t *e) {
    if (e == NULL) {
        return Qnil;
    }
    z_owned_string_t s;
    z_encoding_to_string(e, &s);
    VALUE out = zrb_str_from_view(z_loan(s));
    z_drop(z_move(s));
    return out;
}

/* An owned encoding from a String (already checked). false when there is
 * none to send. */
static bool zrb_encoding_make(z_owned_encoding_t *out, VALUE str) {
    if (NIL_P(str)) {
        return false;
    }
    if (z_encoding_from_str(out, StringValueCStr(str)) != Z_OK) {
        rb_raise(rb_eArgError, "invalid encoding: %" PRIsVALUE, str);
    }
    return true;
}

/* ---------------------------------------------------------- timestamps */

typedef struct {
    z_timestamp_t ts;
} zrb_ts;

static size_t zrb_ts_size(const void *p) { return sizeof(zrb_ts); }

static const rb_data_type_t zrb_ts_type = {
    "Asterism::Zenoh::Timestamp", {NULL, RUBY_TYPED_DEFAULT_FREE, zrb_ts_size}, NULL, NULL, RUBY_TYPED_FREE_IMMEDIATELY};

static bool zrb_is_timestamp(VALUE v) { return rb_typeddata_is_kind_of(v, &zrb_ts_type); }

static VALUE zrb_ts_wrap(const z_timestamp_t *ts) {
    zrb_ts *t;
    VALUE obj = TypedData_Make_Struct(cTimestamp, zrb_ts, &zrb_ts_type, t);
    t->ts = *ts;
    return obj;
}

static zrb_ts *zrb_ts_get(VALUE self) {
    zrb_ts *t;
    TypedData_Get_Struct(self, zrb_ts, &zrb_ts_type, t);
    return t;
}

/* NTP64 of zenoh's HLC: seconds since the UNIX epoch in the upper 32 bits,
 * the fraction of a second in the lower 32. */
static VALUE zrb_ts_ntp64(VALUE self) { return ULL2NUM(z_timestamp_ntp64_time(&zrb_ts_get(self)->ts)); }

static VALUE zrb_ts_id(VALUE self) {
    z_id_t id = z_timestamp_id(&zrb_ts_get(self)->ts);
    return zrb_zid_value(&id);
}

static VALUE zrb_ts_to_time(VALUE self) {
    uint64_t t = z_timestamp_ntp64_time(&zrb_ts_get(self)->ts);
    time_t sec = (time_t)(t >> 32);
    long nsec = (long)(((t & 0xffffffffULL) * 1000000000ULL) >> 32);
    return rb_time_nano_new(sec, nsec);
}

static VALUE zrb_ts_cmp(VALUE self, VALUE other) {
    if (!zrb_is_timestamp(other)) {
        return Qnil;
    }
    const z_timestamp_t *a = &zrb_ts_get(self)->ts, *b = &zrb_ts_get(other)->ts;
    uint64_t ta = z_timestamp_ntp64_time(a), tb = z_timestamp_ntp64_time(b);
    if (ta != tb) {
        return INT2FIX(ta < tb ? -1 : 1);
    }
    z_id_t ia = z_timestamp_id(a), ib = z_timestamp_id(b);
    /* LSB first: compare from the most significant byte. */
    for (int i = 15; i >= 0; i--) {
        if (ia.id[i] != ib.id[i]) {
            return INT2FIX(ia.id[i] < ib.id[i] ? -1 : 1);
        }
    }
    return INT2FIX(0);
}

static VALUE zrb_ts_eql(VALUE self, VALUE other) {
    VALUE c = zrb_ts_cmp(self, other);
    return (!NIL_P(c) && FIX2INT(c) == 0) ? Qtrue : Qfalse;
}

static VALUE zrb_ts_hash(VALUE self) {
    return rb_funcall(rb_ary_new_from_args(2, zrb_ts_ntp64(self), zrb_ts_id(self)), rb_intern("hash"), 0);
}

/* "<ntp64>/<id>" as zenoh prints it, with the time in seconds. */
static VALUE zrb_ts_to_s(VALUE self) {
    VALUE t = zrb_ts_to_time(self);
    VALUE iso = rb_funcall(rb_funcall(t, rb_intern("utc"), 0), rb_intern("strftime"), 1,
                           rb_str_new_cstr("%Y-%m-%dT%H:%M:%S.%9NZ"));
    return rb_sprintf("%" PRIsVALUE "/%" PRIsVALUE, iso, zrb_ts_id(self));
}

static VALUE zrb_ts_inspect(VALUE self) { return rb_sprintf("#<Asterism::Zenoh::Timestamp %" PRIsVALUE ">", zrb_ts_to_s(self)); }

/* -------------------------------------------------------------- samples */

/* A sample as an Asterism::Zenoh::Sample (key, payload, attachment, kind,
 * encoding, timestamp, priority, congestion_control, express,
 * reliability, source_zid). */
static VALUE zrb_sample_value(const z_loaned_sample_t *s) {
    VALUE h = rb_hash_new();
    zrb_hset(h, "key", zrb_key_str(z_sample_keyexpr(s)));
    zrb_hset(h, "payload", zrb_str_from_bytes(z_sample_payload(s)));
    zrb_hset(h, "attachment", zrb_attachment_value(z_sample_attachment(s)));
    zrb_hset(h, "kind", ID2SYM(rb_intern(z_sample_kind(s) == Z_SAMPLE_KIND_DELETE ? "delete" : "put")));
    zrb_hset(h, "encoding", zrb_encoding_value(z_sample_encoding(s)));
    const z_timestamp_t *ts = z_sample_timestamp(s);
    zrb_hset(h, "timestamp", ts != NULL ? zrb_ts_wrap(ts) : Qnil);
    zrb_hset(h, "priority", zrb_priority_sym((int)z_sample_priority(s)));
    zrb_hset(h, "congestion_control", zrb_cc_sym((int)z_sample_congestion_control(s)));
    zrb_hset(h, "express", z_sample_express(s) ? Qtrue : Qfalse);
    zrb_hset(h, "reliability", zrb_reliability_sym((int)z_sample_reliability(s)));
    const z_source_info_t *si = z_sample_source_info(s);
    if (si != NULL) {
        z_entity_global_id_t gid = z_source_info_id(si);
        z_id_t zid = z_entity_global_id_zid(&gid);
        zrb_hset(h, "source_zid", zrb_zid_value(&zid));
    }
    return zrb_new_value("Sample", h);
}

/* A reply as an Asterism::Zenoh::Reply: an answer (the sample's fields)
 * or an error (error: true, payload and encoding of the error). */
static VALUE zrb_reply_value(const z_loaned_reply_t *r) {
    VALUE h = rb_hash_new();
    if (z_reply_is_ok(r)) {
        const z_loaned_sample_t *s = z_reply_ok(r);
        zrb_hset(h, "key", zrb_key_str(z_sample_keyexpr(s)));
        zrb_hset(h, "payload", zrb_str_from_bytes(z_sample_payload(s)));
        zrb_hset(h, "attachment", zrb_attachment_value(z_sample_attachment(s)));
        zrb_hset(h, "kind", ID2SYM(rb_intern(z_sample_kind(s) == Z_SAMPLE_KIND_DELETE ? "delete" : "put")));
        zrb_hset(h, "encoding", zrb_encoding_value(z_sample_encoding(s)));
        const z_timestamp_t *ts = z_sample_timestamp(s);
        zrb_hset(h, "timestamp", ts != NULL ? zrb_ts_wrap(ts) : Qnil);
        zrb_hset(h, "error", Qfalse);
    } else {
        const z_loaned_reply_err_t *e = z_reply_err(r);
        zrb_hset(h, "key", Qnil);
        zrb_hset(h, "payload", zrb_str_from_bytes(z_reply_err_payload(e)));
        zrb_hset(h, "encoding", zrb_encoding_value(z_reply_err_encoding(e)));
        zrb_hset(h, "error", Qtrue);
    }
    z_entity_global_id_t gid;
    if (z_reply_replier_id(r, &gid)) {
        z_id_t zid = z_entity_global_id_zid(&gid);
        zrb_hset(h, "replier_zid", zrb_zid_value(&zid));
    }
    return zrb_new_value("Reply", h);
}

/* -------------------------------------------------------- key expressions */

typedef struct {
    zrb_ent ent;            /* declared ones only */
    z_owned_keyexpr_t ke;   /* always valid */
    z_owned_keyexpr_t decl; /* valid while ent.declared */
} zrb_ke;

static void zrb_ke_undeclare(zrb_ent *e) {
    zrb_ke *k = (zrb_ke *)e;
    if (e->owner != NULL && e->owner->live) {
        z_undeclare_keyexpr(z_loan(e->owner->session), z_move(k->decl));
    } else {
        z_drop(z_move(k->decl));
    }
}

static void zrb_ke_free(void *p) {
    zrb_ke *k = (zrb_ke *)p;
    zrb_ent_detach(&k->ent);
    z_drop(z_move(k->ke));
    xfree(k);
}

static size_t zrb_ke_size(const void *p) { return sizeof(zrb_ke); }

static const rb_data_type_t zrb_ke_type = {"Asterism::Zenoh::KeyExpr", {NULL, zrb_ke_free, zrb_ke_size}, NULL, NULL, 0};

static zrb_ke *zrb_ke_of(VALUE v) {
    if (!rb_typeddata_is_kind_of(v, &zrb_ke_type)) {
        return NULL;
    }
    return (zrb_ke *)RTYPEDDATA_DATA(v);
}

static const z_loaned_keyexpr_t *zrb_ke_loan(zrb_ke *k) { return k->ent.declared ? z_loan(k->decl) : z_loan(k->ke); }

/* A key argument (String or KeyExpr) as a loaned key expression; view is
 * the storage for a String. */
static const z_loaned_keyexpr_t *zrb_key_loan(VALUE v, z_view_keyexpr_t *view) {
    zrb_ke *k = zrb_ke_of(v);
    if (k != NULL) {
        return zrb_ke_loan(k);
    }
    zrb_view_key(view, StringValueCStr(v));
    return z_loan(*view);
}

/* An owned copy of a key argument, for calls without the GVL. A declared
 * KeyExpr keeps its declaration. */
static void zrb_key_owned(VALUE v, z_owned_keyexpr_t *out) {
    zrb_ke *k = zrb_ke_of(v);
    if (k != NULL) {
        z_keyexpr_clone(out, zrb_ke_loan(k));
        return;
    }
    zrb_owned_key(out, StringValueCStr(v));
}

/* The key argument as a String (to keep on the Ruby objects). */
static VALUE zrb_key_string(VALUE v) {
    zrb_ke *k = zrb_ke_of(v);
    if (k != NULL) {
        return zrb_key_str(z_loan(k->ke));
    }
    StringValueCStr(v);
    return v;
}

static VALUE zrb_ke_wrap_owned(z_owned_keyexpr_t *ke) {
    zrb_ke *k;
    VALUE obj = TypedData_Make_Struct(cKeyExpr, zrb_ke, &zrb_ke_type, k);
    k->ke = *ke;
    z_internal_keyexpr_null(ke);
    z_internal_keyexpr_null(&k->decl);
    return obj;
}

/* KeyExpr.new(str, autocanonize: false). Raises ArgumentError for a key
 * expression that is not valid (or not canonical, without autocanonize). */
static VALUE zrb_ke_s_new(int argc, VALUE *argv, VALUE klass) {
    VALUE str, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &str, &opts);
    VALUE kw[1] = {Qundef};
    if (!NIL_P(opts)) {
        ID ids[1] = {rb_intern("autocanonize")};
        rb_get_kwargs(opts, ids, 0, 1, kw);
    }
    zrb_ke *src = zrb_ke_of(str);
    z_owned_keyexpr_t ke;
    if (src != NULL) {
        z_keyexpr_clone(&ke, z_loan(src->ke));
    } else {
        const char *s = StringValueCStr(str);
        z_result_t r = (kw[0] != Qundef && RTEST(kw[0])) ? z_keyexpr_from_str_autocanonize(&ke, s)
                                                          : z_keyexpr_from_str(&ke, s);
        if (r != Z_OK) {
            rb_raise(rb_eArgError, "invalid key expression: %s", s);
        }
    }
    return zrb_ke_wrap_owned(&ke);
}

static zrb_ke *zrb_ke_get(VALUE self) {
    zrb_ke *k;
    TypedData_Get_Struct(self, zrb_ke, &zrb_ke_type, k);
    return k;
}

static VALUE zrb_ke_to_s(VALUE self) { return zrb_key_str(z_loan(zrb_ke_get(self)->ke)); }

static VALUE zrb_ke_inspect(VALUE self) {
    zrb_ke *k = zrb_ke_get(self);
    return rb_sprintf("#<Asterism::Zenoh::KeyExpr %" PRIsVALUE "%s>", zrb_ke_to_s(self),
                      k->ent.declared ? " (declared)" : "");
}

static VALUE zrb_ke_intersects_p(VALUE self, VALUE other) {
    z_view_keyexpr_t v;
    return z_keyexpr_intersects(z_loan(zrb_ke_get(self)->ke), zrb_key_loan(other, &v)) ? Qtrue : Qfalse;
}

static VALUE zrb_ke_includes_p(VALUE self, VALUE other) {
    z_view_keyexpr_t v;
    return z_keyexpr_includes(z_loan(zrb_ke_get(self)->ke), zrb_key_loan(other, &v)) ? Qtrue : Qfalse;
}

static VALUE zrb_ke_relation_to(VALUE self, VALUE other) {
    z_view_keyexpr_t v;
    switch (z_keyexpr_relation_to(z_loan(zrb_ke_get(self)->ke), zrb_key_loan(other, &v))) {
    case Z_KEYEXPR_INTERSECTION_LEVEL_DISJOINT:
        return ID2SYM(rb_intern("disjoint"));
    case Z_KEYEXPR_INTERSECTION_LEVEL_INTERSECTS:
        return ID2SYM(rb_intern("intersects"));
    case Z_KEYEXPR_INTERSECTION_LEVEL_INCLUDES:
        return ID2SYM(rb_intern("includes"));
    case Z_KEYEXPR_INTERSECTION_LEVEL_EQUALS:
        return ID2SYM(rb_intern("equals"));
    }
    return Qnil;
}

static VALUE zrb_ke_eq(VALUE self, VALUE other) {
    if (zrb_ke_of(other) == NULL && !RB_TYPE_P(other, T_STRING)) {
        return Qfalse;
    }
    z_view_keyexpr_t v;
    if (RB_TYPE_P(other, T_STRING) && z_view_keyexpr_from_str(&v, StringValueCStr(other)) != Z_OK) {
        return Qfalse;
    }
    const z_loaned_keyexpr_t *o = RB_TYPE_P(other, T_STRING) ? z_loan(v) : z_loan(zrb_ke_of(other)->ke);
    return z_keyexpr_equals(z_loan(zrb_ke_get(self)->ke), o) ? Qtrue : Qfalse;
}

static VALUE zrb_ke_hash(VALUE self) { return LONG2NUM((long)rb_str_hash(zrb_ke_to_s(self))); }

/* ke.join(other) -> KeyExpr: "a/b" join "c/d" is "a/b/c/d" (canonized). */
static VALUE zrb_ke_join(VALUE self, VALUE other) {
    z_view_keyexpr_t v;
    const z_loaned_keyexpr_t *o = zrb_key_loan(other, &v);
    z_owned_keyexpr_t out;
    if (z_keyexpr_join(&out, z_loan(zrb_ke_get(self)->ke), o) != Z_OK) {
        rb_raise(rb_eArgError, "cannot join the key expressions");
    }
    return zrb_ke_wrap_owned(&out);
}

/* ke.concat(str) -> KeyExpr: the string appended as it is ("a/b" concat
 * "c" is "a/bc"); the result must be a valid key expression. */
static VALUE zrb_ke_concat(VALUE self, VALUE str) {
    StringValue(str);
    z_owned_keyexpr_t out;
    if (z_keyexpr_concat(&out, z_loan(zrb_ke_get(self)->ke), RSTRING_PTR(str), (size_t)RSTRING_LEN(str)) != Z_OK) {
        rb_raise(rb_eArgError, "cannot concatenate %" PRIsVALUE, str);
    }
    return zrb_ke_wrap_owned(&out);
}

static VALUE zrb_ke_declared_p(VALUE self) { return zrb_ke_get(self)->ent.declared ? Qtrue : Qfalse; }

/* Undeclares a declared key expression (it stays usable as a plain one). */
static VALUE zrb_ke_undeclare_m(VALUE self) {
    zrb_ent_detach(&zrb_ke_get(self)->ent);
    return Qnil;
}

/* KeyExpr.canonize(str) -> String in canonical form (for example two
 * "**" chunks in a row become one, a lone "$*" chunk becomes "*").
 * Raises ArgumentError when it cannot be made one. */
static VALUE zrb_ke_s_canonize(VALUE klass, VALUE str) {
    VALUE out = rb_str_dup(StringValue(str));
    size_t len = (size_t)RSTRING_LEN(out);
    rb_str_modify(out);
    if (z_keyexpr_canonize(RSTRING_PTR(out), &len) != Z_OK) {
        rb_raise(rb_eArgError, "invalid key expression: %" PRIsVALUE, str);
    }
    rb_str_set_len(out, (long)len);
    return out;
}

/* KeyExpr.valid?(str): a key expression in canonical form. */
static VALUE zrb_ke_s_valid_p(VALUE klass, VALUE str) {
    StringValue(str);
    return z_keyexpr_is_canon(RSTRING_PTR(str), (size_t)RSTRING_LEN(str)) == Z_OK ? Qtrue : Qfalse;
}

/* ----------------------------------------------------------- subscriber */

static void zrb_sub_detach(zrb_sub *s) {
    if (s->declared) {
        if (s->type == ZSUB_ADVANCED) {
            z_drop(z_move(s->u.adv));
        } else {
            z_drop(z_move(s->u.sub));
        }
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

/* sub.each_sample { |sample| ... } -> Integer; without a block, Array of
 * Asterism::Zenoh::Sample (the payload with kind, encoding, timestamp,
 * priority and the other fields). Takes from the same queue as
 * each_pending. */
static VALUE zrb_sub_each_sample(VALUE self) {
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
        VALUE v = zrb_sample_value(z_loan(sm));
        z_drop(z_move(sm));
        taken++;
        if (collect) {
            rb_ary_push(out, v);
        } else {
            rb_yield(v);
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

/* The query's own key (key_v nil) or the given one (String or KeyExpr). */
static const z_loaned_keyexpr_t *zrb_reply_key(zrb_query *zq, VALUE key_v, z_view_keyexpr_t *view) {
    if (NIL_P(key_v)) {
        return z_query_keyexpr(z_loan(zq->query));
    }
    return zrb_key_loan(key_v, view);
}

/* A timestamp option: true makes a new one (needs the session, which the
 * query does not carry: true is only accepted where there is a session). */
static const z_timestamp_t *zrb_opts_timestamp(VALUE v, z_timestamp_t *store, const z_loaned_session_t *session) {
    if (NIL_P(v)) {
        return NULL;
    }
    if (v == Qtrue) {
        if (session == NULL || z_timestamp_new(store, session) != Z_OK) {
            rb_raise(eZenohError, "cannot make a timestamp");
        }
        return store;
    }
    *store = zrb_ts_get(v)->ts;
    return store;
}

static const char *const zrb_reply_kw[] = {"attachment", "encoding", "timestamp", "priority", "congestion_control",
                                           "express"};

/* q.reply(payload, **opts) / q.reply(key, payload, **opts) -> nil.
 * opts: attachment:, encoding: (a String such as "application/json"),
 * timestamp: (an Asterism::Zenoh::Timestamp), priority:,
 * congestion_control:, express:. The key defaults to the query's key; it
 * must match the query's key expression. May be called more than once
 * before the query is finished. */
static VALUE zrb_query_reply(int argc, VALUE *argv, VALUE self) {
    VALUE a1, a2 = Qnil, opts = Qnil;
    int n = rb_scan_args(argc, argv, "11:", &a1, &a2, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_reply_kw, 6, &o);
    if (o.timestamp == Qtrue) {
        rb_raise(rb_eArgError, "timestamp: needs a Timestamp here (session.new_timestamp)");
    }
    zrb_query *zq = zrb_query_get_live(self);
    VALUE key_v = (n == 1) ? Qnil : a1;
    VALUE payload = (n == 1) ? a1 : a2;
    if (!RB_TYPE_P(payload, T_STRING)) {
        rb_raise(rb_eTypeError, "payload must be a String");
    }
    z_view_keyexpr_t ke;
    const z_loaned_keyexpr_t *kp = zrb_reply_key(zq, key_v, &ke);
    z_timestamp_t ts;
    const z_timestamp_t *tsp = zrb_opts_timestamp(o.timestamp, &ts, NULL);
    z_query_reply_options_t ro;
    z_query_reply_options_default(&ro);
    if (o.priority >= 0) {
        ro.priority = (z_priority_t)o.priority;
    }
    if (o.congestion_control >= 0) {
        ro.congestion_control = (z_congestion_control_t)o.congestion_control;
    }
    if (o.express >= 0) {
        ro.is_express = o.express;
    }
    if (tsp != NULL) {
        ro.timestamp = &ts;
    }
    /* Nothing below raises before the reply takes the owned values. */
    z_owned_encoding_t enc;
    if (zrb_encoding_make(&enc, o.encoding)) {
        ro.encoding = z_move(enc);
    }
    z_owned_bytes_t att_bytes;
    if (!NIL_P(o.attachment)) {
        zrb_bytes_from_str(&att_bytes, o.attachment, "attachment");
        ro.attachment = z_move(att_bytes);
    }
    z_owned_bytes_t bytes;
    zrb_bytes_from_str(&bytes, payload, "payload");
    z_result_t ret = z_query_reply(z_loan(zq->query), kp, z_move(bytes), &ro);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "reply failed (%d)%" PRIsVALUE, (int)ret, zrb_last_error());
    }
    return Qnil;
}

/* q.reply_err(payload, encoding: nil) -> nil: an error answer; the
 * requester sees it as an error (Get#errors, each_result's error?). */
static VALUE zrb_query_reply_err(int argc, VALUE *argv, VALUE self) {
    VALUE payload, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &payload, &opts);
    static const char *const kws[] = {"encoding"};
    zrb_opts o;
    zrb_opts_parse(opts, kws, 1, &o);
    if (!RB_TYPE_P(payload, T_STRING)) {
        rb_raise(rb_eTypeError, "payload must be a String");
    }
    zrb_query *zq = zrb_query_get_live(self);
    z_query_reply_err_options_t eo;
    z_query_reply_err_options_default(&eo);
    z_owned_encoding_t enc;
    if (zrb_encoding_make(&enc, o.encoding)) {
        eo.encoding = z_move(enc);
    }
    z_owned_bytes_t bytes;
    zrb_bytes_from_str(&bytes, payload, "payload");
    z_result_t ret = z_query_reply_err(z_loan(zq->query), z_move(bytes), &eo);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "reply_err failed (%d)%" PRIsVALUE, (int)ret, zrb_last_error());
    }
    return Qnil;
}

/* q.reply_del(key = nil, attachment:, timestamp:, priority:,
 * congestion_control:, express:) -> nil: answers that the key was
 * deleted (a DELETE sample). */
static VALUE zrb_query_reply_del(int argc, VALUE *argv, VALUE self) {
    VALUE key_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "01:", &key_v, &opts);
    static const char *const kws[] = {"attachment", "timestamp", "priority", "congestion_control", "express"};
    zrb_opts o;
    zrb_opts_parse(opts, kws, 5, &o);
    if (o.timestamp == Qtrue) {
        rb_raise(rb_eArgError, "timestamp: needs a Timestamp here (session.new_timestamp)");
    }
    zrb_query *zq = zrb_query_get_live(self);
    z_view_keyexpr_t ke;
    const z_loaned_keyexpr_t *kp = zrb_reply_key(zq, key_v, &ke);
    z_timestamp_t ts;
    const z_timestamp_t *tsp = zrb_opts_timestamp(o.timestamp, &ts, NULL);
    z_query_reply_del_options_t d;
    z_query_reply_del_options_default(&d);
    if (o.priority >= 0) {
        d.priority = (z_priority_t)o.priority;
    }
    if (o.congestion_control >= 0) {
        d.congestion_control = (z_congestion_control_t)o.congestion_control;
    }
    if (o.express >= 0) {
        d.is_express = o.express;
    }
    if (tsp != NULL) {
        d.timestamp = &ts;
    }
    z_owned_bytes_t att_bytes;
    if (!NIL_P(o.attachment)) {
        zrb_bytes_from_str(&att_bytes, o.attachment, "attachment");
        d.attachment = z_move(att_bytes);
    }
    z_result_t ret = z_query_reply_del(z_loan(zq->query), kp, &d);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "reply_del failed (%d)%" PRIsVALUE, (int)ret, zrb_last_error());
    }
    return Qnil;
}

/* q.encoding -> String or nil: the encoding of the query's payload. */
static VALUE zrb_query_encoding(VALUE self) {
    zrb_query *zq = zrb_query_get_live(self);
    if (z_query_payload(z_loan(zq->query)) == NULL) {
        return Qnil;
    }
    return zrb_encoding_value(z_query_encoding(z_loan(zq->query)));
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
 * (errors) and taken out, not yielded (each_result returns them). */
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
        if (!z_reply_is_ok(z_loan(r))) {
            /* Counted in errors; each_result returns them. */
            z_drop(z_move(r));
            continue;
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

/* g.each_result { |reply| ... } -> Integer; without a block, Array of
 * Asterism::Zenoh::Reply. Unlike each_reply, error replies are returned
 * too (reply.error?). Takes from the same queue as each_reply. */
static VALUE zrb_get_each_result(VALUE self) {
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
        VALUE v = zrb_reply_value(z_loan(r));
        z_drop(z_move(r));
        taken++;
        if (collect) {
            rb_ary_push(out, v);
        } else {
            rb_yield(v);
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
static void zrb_session_detach_order(zrb_session *z, int order) {
    zrb_ent *e = z->ents;
    while (e != NULL) {
        zrb_ent *next = e->next;
        if (e->order == order) {
            zrb_ent_detach(e); /* unlinks itself */
        }
        e = next;
    }
}

static void zrb_session_detach_all(zrb_session *z) {
    /* Listeners before what they listen to, declared key expressions after
     * everything that may use them. */
    zrb_session_detach_order(z, ZENT_LISTENER);
    zrb_session_detach_order(z, ZENT_ENTITY);
    while (z->qables != NULL) {
        zrb_qable_detach(z->qables); /* unlinks itself */
    }
    while (z->tokens != NULL) {
        zrb_token_detach(z->tokens);
    }
    while (z->subs != NULL) {
        zrb_sub_detach(z->subs);
    }
    zrb_session_detach_order(z, ZENT_KEYEXPR);
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

/* The value at key of a configuration as JSON (a Ruby String), or nil. */
static VALUE zrb_config_get(const z_loaned_config_t *c, const char *key) {
    z_owned_string_t s;
    if (zc_config_get_from_str(c, key, &s) != Z_OK) {
        return Qnil;
    }
    VALUE out = zrb_str_from_view(z_loan(s));
    z_drop(z_move(s));
    return out;
}

/* A Ruby value as JSON text (for the config: Hash). */
static VALUE zrb_to_json(VALUE v) {
    static bool loaded = false;
    if (!loaded) {
        rb_require("json");
        loaded = true;
    }
    return rb_funcall(rb_const_get(rb_cObject, rb_intern("JSON")), rb_intern("generate"), 1, v);
}

typedef struct {
    z_owned_config_t *config;
    z_owned_config_t *dflt; /* zenoh's defaults, when the user gave a file */
} zrb_cfg;

/* One of the gem's own settings: applied unless the user's file (or
 * configuration String) set that key to something other than zenoh's
 * default. */
static bool zrb_cfg_internal(zrb_cfg *c, const char *key, const char *json5) {
    if (c->dflt != NULL) {
        VALUE mine = zrb_config_get(z_loan(*c->config), key);
        VALUE def = zrb_config_get(z_loan(*c->dflt), key);
        if (!rb_equal(mine, def)) {
            return true; /* the user's */
        }
    }
    return zrb_config_set(c->config, key, json5);
}

static VALUE zrb_json_endpoints(const char *ep) {
    return rb_str_concat(rb_str_concat(rb_str_new_cstr("["), zrb_json_str(ep)), rb_str_new_cstr("]"));
}

typedef struct {
    VALUE hash;
    z_owned_config_t *config;
    VALUE bad; /* the key that failed */
} zrb_cfg_hash;

static int zrb_cfg_hash_i(VALUE key, VALUE val, VALUE arg) {
    zrb_cfg_hash *h = (zrb_cfg_hash *)arg;
    VALUE k = rb_obj_as_string(key);
    VALUE json = zrb_to_json(val);
    if (!zrb_config_set(h->config, StringValueCStr(k), StringValueCStr(json))) {
        h->bad = rb_sprintf("%" PRIsVALUE " = %" PRIsVALUE, k, json);
        return ST_STOP;
    }
    return ST_CONTINUE;
}

static VALUE zrb_cfg_hash_body(VALUE arg) {
    zrb_cfg_hash *h = (zrb_cfg_hash *)arg;
    rb_hash_foreach(h->hash, zrb_cfg_hash_i, arg);
    return Qnil;
}

/* Asterism::Zenoh::Session.open(locator = nil, mode: :client, listen: nil,
 *                               scouting: nil, config: nil, config_file: nil) -> Session
 * - client: connects to the router at locator.
 * - peer: connects to the peer at locator (if given) and/or listens on
 *   listen (e.g. "tcp/0.0.0.0:7447").
 * - scouting: true turns multicast scouting (and gossip) on, so a peer or
 *   client finds the others by itself (then no locator is needed).
 * - timestamping: true stamps every put of the session with its clock
 *   (needed by an advanced publisher with a cache but no
 *   sample_miss_detection; zenoh's default is on for routers only).
 * - config: a Hash of zenoh configuration keys and values
 *   ({"transport/link/tls/root_ca_certificate" => "ca.pem"}; the values are
 *   Ruby values, sent as JSON), or a String with a whole JSON5
 *   configuration. config_file: a JSON5 configuration file.
 * What wins, lowest first: zenoh's defaults, the gem's own settings (no
 * scouting, CONNECT_TIMEOUT_MS, SEND_TIMEOUT_MS, a connecting peer does
 * not listen), the file or the configuration String, the arguments
 * (locator, mode, listen, scouting, timestamping), the config Hash.
 * Raises Asterism::Zenoh::Error when the session cannot be opened (no
 * router or peer answered within the connect timeout). */
static VALUE zrb_session_s_open(int argc, VALUE *argv, VALUE klass) {
    VALUE locator_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "01:", &locator_v, &opts);
    VALUE kw[6] = {Qundef, Qundef, Qundef, Qundef, Qundef, Qundef};
    if (!NIL_P(opts)) {
        ID ids[6] = {id_mode, id_listen, id_scouting, id_config, id_config_file, rb_intern("timestamping")};
        rb_get_kwargs(opts, ids, 0, 6, kw);
    }
    int timestamping = (kw[5] == Qundef || NIL_P(kw[5])) ? -1 : (RTEST(kw[5]) ? 1 : 0);
    int mode_arg = -1; /* 0 client, 1 peer */
    if (kw[0] != Qundef && !NIL_P(kw[0])) {
        ID mode = zrb_sym_id(kw[0]);
        if (mode == id_peer) {
            mode_arg = 1;
        } else if (mode == id_client) {
            mode_arg = 0;
        } else {
            rb_raise(rb_eArgError, "mode must be :client or :peer");
        }
    }
    const char *locator = NIL_P(locator_v) ? NULL : StringValueCStr(locator_v);
    VALUE listen_v = (kw[1] == Qundef) ? Qnil : kw[1];
    const char *listen = NIL_P(listen_v) ? NULL : StringValueCStr(listen_v);
    int scouting = (kw[2] == Qundef || NIL_P(kw[2])) ? -1 : (RTEST(kw[2]) ? 1 : 0);
    VALUE config_v = (kw[3] == Qundef) ? Qnil : kw[3];
    VALUE file_v = (kw[4] == Qundef) ? Qnil : kw[4];
    VALUE config_hash = Qnil, config_str = Qnil;
    if (!NIL_P(config_v)) {
        if (RB_TYPE_P(config_v, T_HASH)) {
            config_hash = config_v;
        } else if (RB_TYPE_P(config_v, T_STRING)) {
            config_str = config_v;
        } else {
            rb_raise(rb_eTypeError, "config must be a Hash or a String (JSON5)");
        }
    }
    const char *file = NIL_P(file_v) ? NULL : StringValueCStr(file_v);
    if (file != NULL && !NIL_P(config_str)) {
        rb_raise(rb_eArgError, "config: (a String) and config_file: cannot be used together");
    }
    bool user_base = (file != NULL || !NIL_P(config_str));
    if (mode_arg != 1 && listen != NULL && !user_base && NIL_P(config_hash)) {
        rb_raise(rb_eArgError, "listen needs mode: :peer");
    }
    if (locator == NULL && listen == NULL && scouting != 1 && !user_base && NIL_P(config_hash)) {
        rb_raise(rb_eArgError, "a locator (or, for a peer, listen:) is needed");
    }
    if (!NIL_P(config_hash)) {
        zrb_to_json(Qnil); /* loads json now, before anything is allocated */
    }

    zrb_session *z;
    VALUE obj = TypedData_Make_Struct(klass, zrb_session, &zrb_session_type, z);
    pthread_mutex_init(&z->op_lock, NULL);

    zrb_open_args a;
    a.z = z;
    z_owned_config_t dflt;
    z_result_t cr;
    if (file != NULL) {
        cr = zc_config_from_file(&a.config, file);
    } else if (!NIL_P(config_str)) {
        cr = zc_config_from_str(&a.config, StringValueCStr(config_str));
    } else {
        cr = z_config_default(&a.config);
    }
    if (cr != Z_OK) {
        if (file != NULL) {
            rb_raise(rb_eArgError, "cannot read the configuration file %s (%d)", file, (int)cr);
        }
        rb_raise(rb_eArgError, "invalid configuration (%d)", (int)cr);
    }
    if (user_base && z_config_default(&dflt) != Z_OK) {
        z_drop(z_move(a.config));
        rb_raise(eZenohError, "cannot create the configuration");
    }
    zrb_cfg c = {&a.config, user_base ? &dflt : NULL};

    /* Peer or client, as far as the arguments and the file say (a config
     * Hash may still change it; the mode is read back below). */
    bool peer = (mode_arg == 1);
    if (mode_arg < 0 && user_base) {
        VALUE m = zrb_config_get(z_loan(a.config), "mode");
        peer = !NIL_P(m) && rb_str_equal(m, rb_str_new_cstr("\"peer\"")) == Qtrue;
    }

    /* The gem's own settings mirror the mruby gem's zenoh-pico build: no
     * scouting (the locator is given), a connect-only peer does not listen,
     * and the session fails to open when nobody answers in time. */
    char num[32];
    bool ok = true;
    if (mode_arg >= 0) {
        ok = zrb_config_set(&a.config, "mode", mode_arg == 1 ? "\"peer\"" : "\"client\"");
    } else {
        ok = zrb_cfg_internal(&c, "mode", "\"client\"");
    }
    if (ok && scouting >= 0) {
        ok = zrb_config_set(&a.config, "scouting/multicast/enabled", scouting ? "true" : "false") &&
             zrb_config_set(&a.config, "scouting/gossip/enabled", scouting ? "true" : "false");
    } else if (ok) {
        ok = zrb_cfg_internal(&c, "scouting/multicast/enabled", "false") &&
             zrb_cfg_internal(&c, "scouting/gossip/enabled", "false");
    }
    if (ok) {
        snprintf(num, sizeof(num), "%d", ZRB_CONNECT_TIMEOUT_MS);
        ok = zrb_cfg_internal(&c, "connect/timeout_ms", num) &&
             zrb_cfg_internal(&c, "connect/exit_on_failure", "true");
    }
    if (ok) {
        snprintf(num, sizeof(num), "%d", ZRB_SEND_TIMEOUT_MS * 1000);
        ok = zrb_cfg_internal(&c, "transport/link/tx/queue/congestion_control/block/wait_before_close", num);
    }
    if (ok && timestamping >= 0) {
        ok = zrb_config_set(&a.config, "timestamping/enabled", timestamping ? "true" : "false");
    }
    if (ok && locator != NULL) {
        VALUE ep = zrb_json_endpoints(locator);
        ok = zrb_config_set(&a.config, "connect/endpoints", StringValueCStr(ep));
    }
    if (ok) {
        if (listen != NULL) {
            VALUE ep = zrb_json_endpoints(listen);
            ok = zrb_config_set(&a.config, "listen/endpoints", StringValueCStr(ep));
        } else if (!(peer && scouting == 1)) {
            /* A scouting peer keeps zenoh's own listener, so the others can
             * connect to it. */
            ok = zrb_cfg_internal(&c, "listen/endpoints", "[]");
        }
    }
    if (user_base) {
        z_drop(z_move(dflt));
    }
    if (!ok) {
        z_drop(z_move(a.config));
        rb_raise(eZenohError, "invalid locator");
    }
    if (!NIL_P(config_hash)) {
        zrb_cfg_hash h = {config_hash, &a.config, Qnil};
        int state = 0;
        rb_protect(zrb_cfg_hash_body, (VALUE)&h, &state);
        if (state != 0 || !NIL_P(h.bad)) {
            z_drop(z_move(a.config));
            if (state != 0) {
                rb_jump_tag(state);
            }
            rb_raise(rb_eArgError, "invalid configuration: %" PRIsVALUE, h.bad);
        }
    }

    /* What the session will be, read back from the final configuration. */
    VALUE m = zrb_config_get(z_loan(a.config), "mode");
    bool is_router = !NIL_P(m) && rb_str_equal(m, rb_str_new_cstr("\"router\"")) == Qtrue;
    peer = is_router || (!NIL_P(m) && rb_str_equal(m, rb_str_new_cstr("\"peer\"")) == Qtrue);
    VALUE lst = zrb_config_get(z_loan(a.config), "listen/endpoints");
    VALUE mc = zrb_config_get(z_loan(a.config), "scouting/multicast/enabled");
    bool listens = !NIL_P(lst) && rb_str_equal(lst, rb_str_new_cstr("[]")) != Qtrue &&
                   rb_str_equal(lst, rb_str_new_cstr("{}")) != Qtrue;
    bool scouts = !NIL_P(mc) && rb_str_equal(mc, rb_str_new_cstr("true")) == Qtrue;

    rb_thread_call_without_gvl(zrb_open_nogvl, &a, RUBY_UBF_IO, NULL);
    if (a.ret != Z_OK) {
        rb_raise(eZenohError, "cannot open a session to %s (%d)%" PRIsVALUE,
                 locator != NULL ? locator : (listen != NULL ? listen : "the configured endpoints"), (int)a.ret,
                 zrb_last_error());
    }
    z->open = true;
    z->live = true;
    z->peer = peer;
    /* A peer that listens (or finds the others by scouting) stays open
     * without peers; one that only connects closes when it has none. */
    z->listening = peer && (is_router || listens || scouts);
    return obj;
}

static VALUE zrb_sub_new(VALUE self, zrb_session *z, VALUE key_v, long depth, bool liveliness) {
    const char *key = StringValueCStr(key_v);
    z_view_keyexpr_t ke;
    zrb_view_key(&ke, key);

    zrb_sub *s;
    VALUE obj = TypedData_Make_Struct(liveliness ? cWatch : cSubscriber, zrb_sub, &zrb_sub_type, s);
    s->liveliness = liveliness;
    s->type = liveliness ? ZSUB_LIVELINESS : ZSUB_PLAIN;
    s->ch = zch_new(ZCH_SAMPLE, (uint32_t)depth);

    z_owned_closure_sample_t cb;
    zch_closure_sample(s->ch, &cb);
    z_result_t ret;
    if (liveliness) {
        /* history: the tokens alive now are reported first, as appearing. */
        z_liveliness_subscriber_options_t lo;
        z_liveliness_subscriber_options_default(&lo);
        lo.history = true;
        ret = z_liveliness_declare_subscriber(z_loan(z->session), &s->u.sub, z_loan(ke), z_move(cb), &lo);
    } else {
        /* Remote samples only: zenoh-pico (the mruby gem) does not deliver a
         * session's own puts to its own subscribers either. */
        z_subscriber_options_t so;
        z_subscriber_options_default(&so);
        so.allowed_origin = Z_LOCALITY_REMOTE;
        ret = z_declare_subscriber(z_loan(z->session), &s->u.sub, z_loan(ke), z_move(cb), &so);
    }
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot subscribe to %s (%d)%" PRIsVALUE, key, (int)ret, zrb_last_error());
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
        rb_raise(eZenohError, "cannot declare a queryable on %s (%d)%" PRIsVALUE, key, (int)ret, zrb_last_error());
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

static const char *const zrb_get_kw[] = {"attachment", "target", "consolidation", "encoding",
                                         "priority", "congestion_control", "express", "accept_replies"};

/* session.get(key, timeout_ms = 2000, params = nil, payload = nil,
 *             attachment: nil, target: :all, consolidation: :none,
 *             encoding: nil, priority: nil, congestion_control: nil,
 *             express: nil, accept_replies: nil) -> Get.
 * Returns at once; the replies come in on zenoh's threads. key: a String
 * or a KeyExpr. accept_replies: :matching_query (zenoh's default) or :any. */
static VALUE zrb_session_get_m(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, timeout_v = Qnil, params_v = Qnil, payload = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "13:", &key_v, &timeout_v, &params_v, &payload, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_get_kw, 8, &o);
    VALUE key_str = zrb_key_string(key_v);
    long timeout_ms = zrb_check_timeout(timeout_v);
    const char *params = NIL_P(params_v) ? NULL : StringValueCStr(params_v);
    if (!NIL_P(payload) && !RB_TYPE_P(payload, T_STRING)) {
        rb_raise(rb_eTypeError, "payload must be a String");
    }
    z_query_target_t target = zrb_get_target(o.has_target ? o.target : Qnil);
    z_query_consolidation_t consolidation = zrb_get_consolidation(o.has_consolidation ? o.consolidation : Qnil);
    zrb_session *z = zrb_session_get_open(self);

    zrb_get_args a;
    memset(&a, 0, sizeof(a));
    a.z = z;
    zrb_key_owned(key_v, &a.key);
    zrb_get *g;
    VALUE obj = zrb_get_new(&g);
    z_get_options_default(&a.opts);
    a.opts.timeout_ms = (uint64_t)timeout_ms;
    a.opts.target = target;
    a.opts.consolidation = consolidation;
    /* Remote queryables only, as with zenoh-pico. */
    a.opts.allowed_destination = Z_LOCALITY_REMOTE;
    if (o.priority >= 0) {
        a.opts.priority = (z_priority_t)o.priority;
    }
    if (o.congestion_control >= 0) {
        a.opts.congestion_control = (z_congestion_control_t)o.congestion_control;
    }
    if (o.express >= 0) {
        a.opts.is_express = o.express;
    }
    if (o.accept_replies >= 0) {
        a.opts.accept_replies = (z_reply_keyexpr_t)o.accept_replies;
    }
    z_owned_bytes_t bytes;
    z_owned_bytes_t att_bytes;
    z_owned_encoding_t enc;
    if (!NIL_P(payload)) {
        zrb_bytes_from_str(&bytes, payload, "payload");
        a.opts.payload = z_move(bytes);
    }
    if (!NIL_P(o.attachment)) {
        zrb_bytes_from_str(&att_bytes, o.attachment, "attachment");
        a.opts.attachment = z_move(att_bytes);
    }
    if (zrb_encoding_make(&enc, o.encoding)) {
        a.opts.encoding = z_move(enc);
    }
    if (params != NULL) {
        a.params = strdup(params);
    }
    zch_closure_reply(g->ch, &a.cb);
    return zrb_run_get(self, z, &a, obj, key_str, "get");
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
        rb_raise(eZenohError, "cannot declare a liveliness token on %s (%d)%" PRIsVALUE, key, (int)ret, zrb_last_error());
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

/* ------------------------------------------------- sending (0.3.0 path) */

typedef struct zrb_pub zrb_pub;

struct zrb_pub {
    zrb_ent ent;
    bool advanced;
    union {
        z_owned_publisher_t p;
        ze_owned_advanced_publisher_t a;
    } u;
};

typedef enum { ZS_PUT, ZS_DEL, ZS_PPUT, ZS_PDEL, ZS_APUT, ZS_ADEL } zsend_op;

/* One put or delete, of the session or of a publisher, made without the
 * GVL. Everything owned is null until made, and dropped after the call
 * (a value zenoh-c took is left empty, so dropping it again is a no-op). */
typedef struct {
    zrb_session *z;
    zrb_pub *pub;
    zsend_op op;
    z_owned_keyexpr_t key;
    z_owned_bytes_t payload;
    z_owned_bytes_t att;
    z_owned_encoding_t enc;
    z_timestamp_t ts;
    union {
        z_put_options_t put;
        z_delete_options_t del;
        z_publisher_put_options_t pput;
        z_publisher_delete_options_t pdel;
        ze_advanced_publisher_put_options_t aput;
        ze_advanced_publisher_delete_options_t adel;
    } o;
    bool undeclared; /* the publisher was closed */
    z_result_t ret;
} zrb_send;

static void zrb_send_init(zrb_send *s, zrb_session *z, zrb_pub *pub, zsend_op op) {
    memset(s, 0, sizeof(*s));
    s->z = z;
    s->pub = pub;
    s->op = op;
    s->ret = Z_OK;
    z_internal_keyexpr_null(&s->key);
    z_internal_bytes_null(&s->payload);
    z_internal_bytes_null(&s->att);
    z_internal_encoding_null(&s->enc);
}

static void *zrb_send_nogvl(void *p) {
    zrb_send *s = (zrb_send *)p;
    pthread_mutex_lock(&s->z->op_lock);
    if (!s->z->live) {
        s->ret = Z_ESESSION_CLOSED;
    } else if (s->pub != NULL && !s->pub->ent.declared) {
        s->undeclared = true;
    } else {
        switch (s->op) {
        case ZS_PUT:
            s->ret = z_put(z_loan(s->z->session), z_loan(s->key), z_move(s->payload), &s->o.put);
            break;
        case ZS_DEL:
            s->ret = z_delete(z_loan(s->z->session), z_loan(s->key), &s->o.del);
            break;
        case ZS_PPUT:
            s->ret = z_publisher_put(z_loan(s->pub->u.p), z_move(s->payload), &s->o.pput);
            break;
        case ZS_PDEL:
            s->ret = z_publisher_delete(z_loan(s->pub->u.p), &s->o.pdel);
            break;
        case ZS_APUT:
            s->ret = ze_advanced_publisher_put(z_loan(s->pub->u.a), z_move(s->payload), &s->o.aput);
            break;
        case ZS_ADEL:
            s->ret = ze_advanced_publisher_delete(z_loan(s->pub->u.a), &s->o.adel);
            break;
        }
    }
    pthread_mutex_unlock(&s->z->op_lock);
    return NULL;
}

static void zrb_send_run(zrb_send *s, const char *what) {
    rb_thread_call_without_gvl(zrb_send_nogvl, s, RUBY_UBF_IO, NULL);
    z_drop(z_move(s->key));
    z_drop(z_move(s->payload));
    z_drop(z_move(s->att));
    z_drop(z_move(s->enc));
    if (zrb_session_check_link(s->z)) {
        rb_raise(eZenohError, "%s failed: the connection is lost (%d)", what, (int)s->ret);
    }
    if (s->undeclared) {
        rb_raise(eZenohError, "%s failed: the publisher is closed", what);
    }
    if (s->ret != Z_OK) {
        rb_raise(eZenohError, "%s failed (%d)", what, (int)s->ret);
    }
}

/* The timestamp option (nil, true for a new one, or a Timestamp). */
static z_timestamp_t *zrb_send_timestamp(zrb_send *s, VALUE v) {
    if (NIL_P(v)) {
        return NULL;
    }
    if (v == Qtrue) {
        if (z_timestamp_new(&s->ts, z_loan(s->z->session)) != Z_OK) {
            rb_raise(eZenohError, "cannot make a timestamp");
        }
    } else {
        s->ts = zrb_ts_get(v)->ts;
    }
    return &s->ts;
}

/* Payload, attachment and encoding into s (after every check: only
 * allocation failures raise from here on). */
static void zrb_send_values(zrb_send *s, VALUE payload, const zrb_opts *o) {
    if (!NIL_P(payload)) {
        zrb_bytes_from_str(&s->payload, payload, "payload");
    }
    if (!NIL_P(o->attachment)) {
        zrb_bytes_from_str(&s->att, o->attachment, "attachment");
    }
    zrb_encoding_make(&s->enc, o->encoding);
}

static const char *const zrb_put_kw[] = {"attachment", "encoding", "priority", "congestion_control",
                                         "express", "reliability", "timestamp", "allowed_destination"};
static const char *const zrb_del_kw[] = {"priority", "congestion_control", "express",
                                         "reliability", "timestamp", "allowed_destination"};

/* session.put(key, payload, attachment: nil, encoding: nil, priority: nil,
 *             congestion_control: nil, express: nil, reliability: nil,
 *             timestamp: nil, allowed_destination: nil) -> nil.
 * key: a String or a KeyExpr. encoding: a String ("application/json").
 * priority: :real_time .. :background (or 1..7), congestion_control:
 * :drop / :block / :block_first, reliability: :reliable / :best_effort,
 * timestamp: true (a new one) or a Timestamp, allowed_destination: :any /
 * :remote / :session_local. nil (or left out) is zenoh-c's default. */
static VALUE zrb_session_put(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, payload, opts = Qnil;
    rb_scan_args(argc, argv, "2:", &key_v, &payload, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_put_kw, 8, &o);
    StringValue(payload);
    zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    zrb_send s;
    zrb_send_init(&s, z, NULL, ZS_PUT);
    z_put_options_default(&s.o.put);
    s.o.put.timestamp = zrb_send_timestamp(&s, o.timestamp);
    if (o.priority >= 0) {
        s.o.put.priority = (z_priority_t)o.priority;
    }
    if (o.congestion_control >= 0) {
        s.o.put.congestion_control = (z_congestion_control_t)o.congestion_control;
    }
    if (o.express >= 0) {
        s.o.put.is_express = o.express;
    }
    if (o.reliability >= 0) {
        s.o.put.reliability = (z_reliability_t)o.reliability;
    }
    if (o.destination >= 0) {
        s.o.put.allowed_destination = (z_locality_t)o.destination;
    }
    zrb_key_owned(key_v, &s.key);
    zrb_send_values(&s, payload, &o);
    if (!NIL_P(o.attachment)) {
        s.o.put.attachment = z_move(s.att);
    }
    if (!NIL_P(o.encoding)) {
        s.o.put.encoding = z_move(s.enc);
    }
    zrb_send_run(&s, "put");
    return Qnil;
}

/* session.delete(key, priority:, congestion_control:, express:,
 *                reliability:, timestamp:, allowed_destination:) -> nil.
 * Subscribers get a sample of kind :delete (each_pending gives it with an
 * empty payload). */
static VALUE zrb_session_delete(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &key_v, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_del_kw, 6, &o);
    zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    zrb_send s;
    zrb_send_init(&s, z, NULL, ZS_DEL);
    z_delete_options_default(&s.o.del);
    s.o.del.timestamp = zrb_send_timestamp(&s, o.timestamp);
    if (o.priority >= 0) {
        s.o.del.priority = (z_priority_t)o.priority;
    }
    if (o.congestion_control >= 0) {
        s.o.del.congestion_control = (z_congestion_control_t)o.congestion_control;
    }
    if (o.express >= 0) {
        s.o.del.is_express = o.express;
    }
    if (o.reliability >= 0) {
        s.o.del.reliability = (z_reliability_t)o.reliability;
    }
    if (o.destination >= 0) {
        s.o.del.allowed_destination = (z_locality_t)o.destination;
    }
    zrb_key_owned(key_v, &s.key);
    zrb_send_run(&s, "delete");
    return Qnil;
}

/* ----------------------------------------------------------- publishers */

static void zrb_pub_undeclare(zrb_ent *e) {
    zrb_pub *p = (zrb_pub *)e;
    if (p->advanced) {
        z_drop(z_move(p->u.a));
    } else {
        z_drop(z_move(p->u.p));
    }
}

static void zrb_pub_free(void *ptr) {
    zrb_pub *p = (zrb_pub *)ptr;
    zrb_ent_detach(&p->ent);
    xfree(p);
}

static size_t zrb_pub_size(const void *p) { return sizeof(zrb_pub); }

static const rb_data_type_t zrb_pub_type = {"Asterism::Zenoh::Publisher", {NULL, zrb_pub_free, zrb_pub_size}, NULL, NULL, 0};

static zrb_pub *zrb_pub_get(VALUE self) {
    zrb_pub *p;
    TypedData_Get_Struct(self, zrb_pub, &zrb_pub_type, p);
    return p;
}

/* The publisher, declared, with its session open. */
static zrb_pub *zrb_pub_get_open(VALUE self) {
    zrb_pub *p = zrb_pub_get(self);
    if (!p->ent.declared) {
        rb_raise(eZenohError, "the publisher is closed");
    }
    if (zrb_session_check_link(p->ent.owner)) {
        rb_raise(eZenohError, "session is closed");
    }
    return p;
}

static const char *const zrb_pub_kw[] = {"encoding", "priority", "congestion_control", "express",
                                         "reliability", "allowed_destination"};

static void zrb_pub_options(z_publisher_options_t *po, const zrb_opts *o) {
    if (o->priority >= 0) {
        po->priority = (z_priority_t)o->priority;
    }
    if (o->congestion_control >= 0) {
        po->congestion_control = (z_congestion_control_t)o->congestion_control;
    }
    if (o->express >= 0) {
        po->is_express = o->express;
    }
    if (o->reliability >= 0) {
        po->reliability = (z_reliability_t)o->reliability;
    }
    if (o->destination >= 0) {
        po->allowed_destination = (z_locality_t)o->destination;
    }
}

/* A sub-option: v is nil / false (off), true (on with zenoh's defaults),
 * an Integer (for cache: the number of samples) or a Hash of the given
 * keys. Returns the Hash (or nil), raises for unknown keys. */
static VALUE zrb_subopt_hash(VALUE v, const char *what, const char *const *keys, int n) {
    if (!RB_TYPE_P(v, T_HASH)) {
        return Qnil;
    }
    VALUE ks = rb_funcall(v, rb_intern("keys"), 0);
    for (long i = 0; i < RARRAY_LEN(ks); i++) {
        VALUE k = RARRAY_AREF(ks, i);
        bool known = false;
        if (SYMBOL_P(k)) {
            for (int j = 0; j < n; j++) {
                if (SYM2ID(k) == rb_intern(keys[j])) {
                    known = true;
                }
            }
        }
        if (!known) {
            rb_raise(rb_eArgError, "unknown key in %s: %" PRIsVALUE, what, k);
        }
    }
    return v;
}

static VALUE zrb_hget(VALUE h, const char *key) {
    if (NIL_P(h)) {
        return Qnil;
    }
    return rb_hash_lookup2(h, ID2SYM(rb_intern(key)), Qnil);
}

static uint64_t zrb_ms_arg(VALUE v, const char *what) {
    long long ms = NUM2LL(v);
    if (ms < 0) {
        rb_raise(rb_eArgError, "%s must be 0 or more", what);
    }
    return (uint64_t)ms;
}

static VALUE zrb_pub_wrap(VALUE self, zrb_session *z, zrb_pub *p, VALUE obj, VALUE key_str) {
    zrb_ent_link(&p->ent, z, ZENT_ENTITY, true, zrb_pub_undeclare);
    zrb_hold_session(obj, self, key_str);
    return obj;
}

/* session.publisher(key, encoding:, priority:, congestion_control:,
 *                   express:, reliability:, allowed_destination:) -> Publisher */
static VALUE zrb_session_publisher(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &key_v, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_pub_kw, 6, &o);
    VALUE key_str = zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t view;
    const z_loaned_keyexpr_t *ke = zrb_key_loan(key_v, &view);
    zrb_pub *p;
    VALUE obj = TypedData_Make_Struct(cPublisher, zrb_pub, &zrb_pub_type, p);
    z_publisher_options_t po;
    z_publisher_options_default(&po);
    zrb_pub_options(&po, &o);
    z_owned_encoding_t enc;
    if (zrb_encoding_make(&enc, o.encoding)) {
        po.encoding = z_move(enc);
    }
    z_result_t ret = z_declare_publisher(z_loan(z->session), &p->u.p, ke, &po);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot declare a publisher on %" PRIsVALUE " (%d)%" PRIsVALUE, key_str, (int)ret, zrb_last_error());
    }
    return zrb_pub_wrap(self, z, p, obj, key_str);
}

static const char *const zrb_cache_keys[] = {"max_samples", "priority", "congestion_control", "express"};
static const char *const zrb_smd_keys[] = {"heartbeat", "heartbeat_ms"};

/* session.advanced_publisher(key, cache: nil, sample_miss_detection: nil,
 *                            publisher_detection: false, **publisher options) -> AdvancedPublisher
 * cache: N (keep the last N samples for late subscribers, the history of
 * an advanced subscriber), true (zenoh's default size) or a Hash
 * {max_samples:, priority:, congestion_control:, express:}.
 * sample_miss_detection: true or {heartbeat: :periodic / :sporadic,
 * heartbeat_ms:} (lets advanced subscribers notice and recover lost
 * samples). publisher_detection: true declares a liveliness token that
 * advanced subscribers can watch (detect_publishers). A cache without
 * sample_miss_detection orders the samples by their timestamps: the
 * session needs timestamping (Session.open(..., timestamping: true)). */
static VALUE zrb_session_advanced_publisher(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &key_v, &opts);
    static const char *const kws[] = {"encoding", "priority", "congestion_control", "express", "reliability",
                                      "allowed_destination", "cache", "sample_miss_detection",
                                      "publisher_detection"};
    VALUE extra[3] = {Qnil, Qnil, Qnil};
    VALUE base = Qnil;
    if (!NIL_P(opts)) {
        /* Split our three keywords from the publisher's ones. */
        base = rb_hash_dup(opts);
        for (int i = 0; i < 3; i++) {
            extra[i] = rb_hash_delete(base, ID2SYM(rb_intern(kws[6 + i])));
        }
    }
    zrb_opts o;
    zrb_opts_parse(base, kws, 6, &o);
    VALUE cache_h = zrb_subopt_hash(extra[0], "cache", zrb_cache_keys, 4);
    VALUE smd_h = zrb_subopt_hash(extra[1], "sample_miss_detection", zrb_smd_keys, 2);

    ze_advanced_publisher_options_t ao;
    ze_advanced_publisher_options_default(&ao);
    zrb_pub_options(&ao.publisher_options, &o);
    if (RTEST(extra[0])) {
        ao.cache.is_enabled = true;
        if (RB_INTEGER_TYPE_P(extra[0])) {
            long n = NUM2LONG(extra[0]);
            if (n < 1) {
                rb_raise(rb_eArgError, "cache must be 1 or more samples");
            }
            ao.cache.max_samples = (size_t)n;
        } else if (!NIL_P(cache_h)) {
            VALUE v;
            if (!NIL_P(v = zrb_hget(cache_h, "max_samples"))) {
                ao.cache.max_samples = (size_t)NUM2ULONG(v);
            }
            if (!NIL_P(v = zrb_hget(cache_h, "priority"))) {
                ao.cache.priority = (z_priority_t)zrb_priority_arg(v);
            }
            if (!NIL_P(v = zrb_hget(cache_h, "congestion_control"))) {
                ao.cache.congestion_control = (z_congestion_control_t)zrb_cc_arg(v);
            }
            if (!NIL_P(v = zrb_hget(cache_h, "express"))) {
                ao.cache.is_express = RTEST(v);
            }
        } else if (extra[0] != Qtrue) {
            rb_raise(rb_eTypeError, "cache must be an Integer, true or a Hash");
        }
    }
    if (RTEST(extra[1])) {
        ao.sample_miss_detection.is_enabled = true;
        if (!NIL_P(smd_h)) {
            VALUE v;
            if (!NIL_P(v = zrb_hget(smd_h, "heartbeat"))) {
                ID hb = zrb_sym_id(v);
                if (hb == rb_intern("periodic")) {
                    ao.sample_miss_detection.heartbeat_mode = ZE_ADVANCED_PUBLISHER_HEARTBEAT_MODE_PERIODIC;
                } else if (hb == rb_intern("sporadic")) {
                    ao.sample_miss_detection.heartbeat_mode = ZE_ADVANCED_PUBLISHER_HEARTBEAT_MODE_SPORADIC;
                } else if (hb == id_none) {
                    ao.sample_miss_detection.heartbeat_mode = ZE_ADVANCED_PUBLISHER_HEARTBEAT_MODE_NONE;
                } else {
                    rb_raise(rb_eArgError, "heartbeat must be :periodic, :sporadic or :none");
                }
            }
            if (!NIL_P(v = zrb_hget(smd_h, "heartbeat_ms"))) {
                ao.sample_miss_detection.heartbeat_period_ms = zrb_ms_arg(v, "heartbeat_ms");
            }
        } else if (extra[1] != Qtrue) {
            rb_raise(rb_eTypeError, "sample_miss_detection must be true or a Hash");
        }
    }
    ao.publisher_detection = RTEST(extra[2]);

    VALUE key_str = zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t view;
    const z_loaned_keyexpr_t *ke = zrb_key_loan(key_v, &view);
    zrb_pub *p;
    VALUE obj = TypedData_Make_Struct(cAdvPublisher, zrb_pub, &zrb_pub_type, p);
    p->advanced = true;
    z_owned_encoding_t enc;
    if (zrb_encoding_make(&enc, o.encoding)) {
        ao.publisher_options.encoding = z_move(enc);
    }
    z_result_t ret = ze_declare_advanced_publisher(z_loan(z->session), &p->u.a, ke, &ao);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot declare an advanced publisher on %" PRIsVALUE " (%d)%" PRIsVALUE, key_str, (int)ret, zrb_last_error());
    }
    return zrb_pub_wrap(self, z, p, obj, key_str);
}

static const char *const zrb_pput_kw[] = {"attachment", "encoding", "timestamp"};

/* pub.put(payload, attachment: nil, encoding: nil, timestamp: nil) -> nil */
static VALUE zrb_pub_put(int argc, VALUE *argv, VALUE self) {
    VALUE payload, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &payload, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_pput_kw, 3, &o);
    StringValue(payload);
    zrb_pub *p = zrb_pub_get_open(self);
    zrb_send s;
    zrb_send_init(&s, p->ent.owner, p, p->advanced ? ZS_APUT : ZS_PPUT);
    z_publisher_put_options_t *po;
    if (p->advanced) {
        ze_advanced_publisher_put_options_default(&s.o.aput);
        po = &s.o.aput.put_options;
    } else {
        z_publisher_put_options_default(&s.o.pput);
        po = &s.o.pput;
    }
    po->timestamp = zrb_send_timestamp(&s, o.timestamp);
    zrb_send_values(&s, payload, &o);
    if (!NIL_P(o.attachment)) {
        po->attachment = z_move(s.att);
    }
    if (!NIL_P(o.encoding)) {
        po->encoding = z_move(s.enc);
    }
    zrb_send_run(&s, "put");
    return Qnil;
}

/* pub.delete(timestamp: nil) -> nil */
static VALUE zrb_pub_delete(int argc, VALUE *argv, VALUE self) {
    VALUE opts = Qnil;
    rb_scan_args(argc, argv, "0:", &opts);
    static const char *const kws[] = {"timestamp"};
    zrb_opts o;
    zrb_opts_parse(opts, kws, 1, &o);
    zrb_pub *p = zrb_pub_get_open(self);
    zrb_send s;
    zrb_send_init(&s, p->ent.owner, p, p->advanced ? ZS_ADEL : ZS_PDEL);
    z_publisher_delete_options_t *po;
    if (p->advanced) {
        ze_advanced_publisher_delete_options_default(&s.o.adel);
        po = &s.o.adel.delete_options;
    } else {
        z_publisher_delete_options_default(&s.o.pdel);
        po = &s.o.pdel;
    }
    po->timestamp = zrb_send_timestamp(&s, o.timestamp);
    zrb_send_run(&s, "delete");
    return Qnil;
}

/* pub.matching? -> true when at least one subscriber matches the key now. */
static VALUE zrb_pub_matching_p(VALUE self) {
    zrb_pub *p = zrb_pub_get_open(self);
    z_matching_status_t st = {false};
    z_result_t r = p->advanced ? ze_advanced_publisher_get_matching_status(z_loan(p->u.a), &st)
                               : z_publisher_get_matching_status(z_loan(p->u.p), &st);
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot get the matching status (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    return st.matching ? Qtrue : Qfalse;
}

static VALUE zrb_pub_close(VALUE self) {
    zrb_ent_detach(&zrb_pub_get(self)->ent);
    return Qnil;
}

static VALUE zrb_pub_closed_p(VALUE self) { return zrb_pub_get(self)->ent.declared ? Qfalse : Qtrue; }

/* ------------------------------------------------------------- queriers */

typedef struct {
    zrb_ent ent;
    z_owned_querier_t q;
} zrb_querier;

static void zrb_querier_undeclare(zrb_ent *e) { z_drop(z_move(((zrb_querier *)e)->q)); }

static void zrb_querier_free(void *ptr) {
    zrb_querier *q = (zrb_querier *)ptr;
    zrb_ent_detach(&q->ent);
    xfree(q);
}

static size_t zrb_querier_size(const void *p) { return sizeof(zrb_querier); }

static const rb_data_type_t zrb_querier_type = {
    "Asterism::Zenoh::Querier", {NULL, zrb_querier_free, zrb_querier_size}, NULL, NULL, 0};

static zrb_querier *zrb_querier_get(VALUE self) {
    zrb_querier *q;
    TypedData_Get_Struct(self, zrb_querier, &zrb_querier_type, q);
    return q;
}

static zrb_querier *zrb_querier_get_open(VALUE self) {
    zrb_querier *q = zrb_querier_get(self);
    if (!q->ent.declared) {
        rb_raise(eZenohError, "the querier is closed");
    }
    if (zrb_session_check_link(q->ent.owner)) {
        rb_raise(eZenohError, "session is closed");
    }
    return q;
}

static const char *const zrb_querier_kw[] = {"target", "consolidation", "timeout_ms", "congestion_control",
                                             "priority", "express", "accept_replies", "allowed_destination"};

/* session.querier(key, target: :all, consolidation: :none,
 *                 timeout_ms: 2000, congestion_control:, priority:,
 *                 express:, accept_replies: :matching_query,
 *                 allowed_destination: :remote) -> Querier.
 * A declared get: q.get(...) sends the same query again and again. */
static VALUE zrb_session_querier(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, opts = Qnil;
    rb_scan_args(argc, argv, "1:", &key_v, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_querier_kw, 8, &o);
    z_querier_options_t qo;
    z_querier_options_default(&qo);
    qo.target = zrb_get_target(o.has_target ? o.target : Qnil);
    qo.consolidation = zrb_get_consolidation(o.has_consolidation ? o.consolidation : Qnil);
    qo.timeout_ms = (uint64_t)zrb_check_timeout(o.has_timeout ? o.timeout : Qnil);
    /* Remote queryables only by default, as with get. */
    qo.allowed_destination = o.destination >= 0 ? (z_locality_t)o.destination : Z_LOCALITY_REMOTE;
    if (o.congestion_control >= 0) {
        qo.congestion_control = (z_congestion_control_t)o.congestion_control;
    }
    if (o.priority >= 0) {
        qo.priority = (z_priority_t)o.priority;
    }
    if (o.express >= 0) {
        qo.is_express = o.express;
    }
    if (o.accept_replies >= 0) {
        qo.accept_replies = (z_reply_keyexpr_t)o.accept_replies;
    }
    VALUE key_str = zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t view;
    const z_loaned_keyexpr_t *ke = zrb_key_loan(key_v, &view);
    zrb_querier *q;
    VALUE obj = TypedData_Make_Struct(cQuerier, zrb_querier, &zrb_querier_type, q);
    z_result_t ret = z_declare_querier(z_loan(z->session), &q->q, ke, &qo);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot declare a querier on %" PRIsVALUE " (%d)%" PRIsVALUE, key_str, (int)ret, zrb_last_error());
    }
    zrb_ent_link(&q->ent, z, ZENT_ENTITY, true, zrb_querier_undeclare);
    zrb_hold_session(obj, self, key_str);
    return obj;
}

typedef struct {
    zrb_session *z;
    zrb_querier *q;
    char *params;
    z_owned_closure_reply_t cb;
    z_owned_bytes_t payload, att;
    z_owned_encoding_t enc;
    z_querier_get_options_t opts;
    bool undeclared;
    z_result_t ret;
} zrb_qget_args;

static void *zrb_qget_nogvl(void *p) {
    zrb_qget_args *a = (zrb_qget_args *)p;
    pthread_mutex_lock(&a->z->op_lock);
    if (!a->z->live) {
        a->ret = Z_ESESSION_CLOSED;
    } else if (!a->q->ent.declared) {
        a->undeclared = true;
    } else {
        a->ret = z_querier_get(z_loan(a->q->q), a->params != NULL ? a->params : "", z_move(a->cb), &a->opts);
    }
    pthread_mutex_unlock(&a->z->op_lock);
    z_drop(z_move(a->cb));
    return NULL;
}

static const char *const zrb_qget_kw[] = {"attachment", "encoding"};

/* querier.get(params = nil, payload = nil, attachment: nil, encoding: nil) -> Get */
static VALUE zrb_querier_get_m(int argc, VALUE *argv, VALUE self) {
    VALUE params_v = Qnil, payload = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "02:", &params_v, &payload, &opts);
    zrb_opts o;
    zrb_opts_parse(opts, zrb_qget_kw, 2, &o);
    const char *params = NIL_P(params_v) ? NULL : StringValueCStr(params_v);
    if (!NIL_P(payload) && !RB_TYPE_P(payload, T_STRING)) {
        rb_raise(rb_eTypeError, "payload must be a String");
    }
    zrb_querier *q = zrb_querier_get_open(self);
    zrb_qget_args a;
    memset(&a, 0, sizeof(a));
    a.z = q->ent.owner;
    a.q = q;
    z_internal_bytes_null(&a.payload);
    z_internal_bytes_null(&a.att);
    z_internal_encoding_null(&a.enc);
    zrb_get *g;
    VALUE obj = zrb_get_new(&g);
    z_querier_get_options_default(&a.opts);
    if (!NIL_P(payload)) {
        zrb_bytes_from_str(&a.payload, payload, "payload");
        a.opts.payload = z_move(a.payload);
    }
    if (!NIL_P(o.attachment)) {
        zrb_bytes_from_str(&a.att, o.attachment, "attachment");
        a.opts.attachment = z_move(a.att);
    }
    if (zrb_encoding_make(&a.enc, o.encoding)) {
        a.opts.encoding = z_move(a.enc);
    }
    if (params != NULL) {
        a.params = strdup(params);
    }
    zch_closure_reply(g->ch, &a.cb);
    rb_thread_call_without_gvl(zrb_qget_nogvl, &a, RUBY_UBF_IO, NULL);
    free(a.params);
    z_drop(z_move(a.payload));
    z_drop(z_move(a.att));
    z_drop(z_move(a.enc));
    if (zrb_session_check_link(a.z)) {
        rb_raise(eZenohError, "get failed: the connection is lost (%d)", (int)a.ret);
    }
    if (a.undeclared) {
        rb_raise(eZenohError, "get failed: the querier is closed");
    }
    if (a.ret != Z_OK) {
        rb_raise(eZenohError, "get failed (%d)", (int)a.ret);
    }
    rb_ivar_set(obj, id_iv_session, rb_ivar_get(self, id_iv_session));
    return obj;
}

static VALUE zrb_querier_matching_p(VALUE self) {
    zrb_querier *q = zrb_querier_get_open(self);
    z_matching_status_t st = {false};
    z_result_t r = z_querier_get_matching_status(z_loan(q->q), &st);
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot get the matching status (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    return st.matching ? Qtrue : Qfalse;
}

static VALUE zrb_querier_close(VALUE self) {
    zrb_ent_detach(&zrb_querier_get(self)->ent);
    return Qnil;
}

static VALUE zrb_querier_closed_p(VALUE self) { return zrb_querier_get(self)->ent.declared ? Qfalse : Qtrue; }

/* ------------------------------------------------------------ listeners */

typedef enum { ZLST_MATCHING, ZLST_TRANSPORT, ZLST_LINK, ZLST_MISS } zlst_type;

typedef struct {
    zrb_ent ent;
    zlst_type type;
    union {
        z_owned_matching_listener_t m;
        z_owned_transport_events_listener_t t;
        z_owned_link_events_listener_t l;
        ze_owned_sample_miss_listener_t s;
    } u;
    zevq *q;
} zrb_lst;

static void zrb_lst_undeclare(zrb_ent *e) {
    zrb_lst *l = (zrb_lst *)e;
    switch (l->type) {
    case ZLST_MATCHING:
        z_drop(z_move(l->u.m));
        break;
    case ZLST_TRANSPORT:
        z_drop(z_move(l->u.t));
        break;
    case ZLST_LINK:
        z_drop(z_move(l->u.l));
        break;
    case ZLST_MISS:
        z_drop(z_move(l->u.s));
        break;
    }
}

static void zrb_lst_free(void *p) {
    zrb_lst *l = (zrb_lst *)p;
    zrb_ent_detach(&l->ent);
    zevq_unref(l->q);
    xfree(l);
}

static size_t zrb_lst_size(const void *p) { return sizeof(zrb_lst); }

static const rb_data_type_t zrb_lst_type = {"Asterism::Zenoh::Listener", {NULL, zrb_lst_free, zrb_lst_size}, NULL, NULL, 0};

static zrb_lst *zrb_lst_get(VALUE self) {
    zrb_lst *l;
    TypedData_Get_Struct(self, zrb_lst, &zrb_lst_type, l);
    return l;
}

static VALUE zrb_lst_new(VALUE klass, zlst_type type, zev_kind kind, VALUE depth_v, zrb_lst **out) {
    long depth = zrb_check_depth(depth_v);
    zrb_lst *l;
    VALUE obj = TypedData_Make_Struct(klass, zrb_lst, &zrb_lst_type, l);
    l->type = type;
    l->q = zevq_new(kind, (uint32_t)depth);
    *out = l;
    return obj;
}

static void zrb_lst_linked(VALUE obj, zrb_lst *l, zrb_session *z, VALUE parent) {
    zrb_ent_link(&l->ent, z, ZENT_LISTENER, false, zrb_lst_undeclare);
    rb_ivar_set(obj, id_iv_parent, parent);
}

/* listener.each_pending { |value| } -> Integer; without a block, an Array.
 * A MatchingListener gives true / false (a subscriber matches or not any
 * more); an EventListener gives TransportEvent, LinkEvent or Miss values. */
static VALUE zrb_lst_each_pending(VALUE self) { return zevq_each_pending(zrb_lst_get(self)->q, true); }
static VALUE zrb_lst_pending(VALUE self) { return UINT2NUM(atomic_load(&zrb_lst_get(self)->q->queued)); }
static VALUE zrb_lst_received(VALUE self) { return UINT2NUM(atomic_load(&zrb_lst_get(self)->q->received)); }
static VALUE zrb_lst_dropped(VALUE self) { return UINT2NUM(atomic_load(&zrb_lst_get(self)->q->dropped)); }

static VALUE zrb_lst_close(VALUE self) {
    zrb_ent_detach(&zrb_lst_get(self)->ent);
    return Qnil;
}

static VALUE zrb_lst_closed_p(VALUE self) { return zrb_lst_get(self)->ent.declared ? Qfalse : Qtrue; }

/* pub.matching_listener(depth = 16) -> MatchingListener: each change of
 * the matching status (true when the first subscriber appears, false when
 * the last goes). Also for queriers (queryables) and advanced publishers. */
static VALUE zrb_pub_matching_listener(int argc, VALUE *argv, VALUE self) {
    VALUE depth_v = Qnil;
    rb_scan_args(argc, argv, "01", &depth_v);
    zrb_pub *p = zrb_pub_get_open(self);
    zrb_lst *l;
    VALUE obj = zrb_lst_new(cMatchingListener, ZLST_MATCHING, ZEV_MATCHING, depth_v, &l);
    z_owned_closure_matching_status_t cb;
    z_closure_matching_status(&cb, zev_on_matching, zevq_on_drop, zevq_ref(l->q));
    z_result_t r = p->advanced
                       ? ze_advanced_publisher_declare_matching_listener(z_loan(p->u.a), &l->u.m, z_move(cb))
                       : z_publisher_declare_matching_listener(z_loan(p->u.p), &l->u.m, z_move(cb));
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot declare a matching listener (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    zrb_lst_linked(obj, l, p->ent.owner, self);
    return obj;
}

static VALUE zrb_querier_matching_listener(int argc, VALUE *argv, VALUE self) {
    VALUE depth_v = Qnil;
    rb_scan_args(argc, argv, "01", &depth_v);
    zrb_querier *q = zrb_querier_get_open(self);
    zrb_lst *l;
    VALUE obj = zrb_lst_new(cMatchingListener, ZLST_MATCHING, ZEV_MATCHING, depth_v, &l);
    z_owned_closure_matching_status_t cb;
    z_closure_matching_status(&cb, zev_on_matching, zevq_on_drop, zevq_ref(l->q));
    z_result_t r = z_querier_declare_matching_listener(z_loan(q->q), &l->u.m, z_move(cb));
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot declare a matching listener (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    zrb_lst_linked(obj, l, q->ent.owner, self);
    return obj;
}

static bool zrb_history_kw(VALUE opts) {
    VALUE kw[1] = {Qundef};
    if (!NIL_P(opts)) {
        rb_get_kwargs(opts, &id_history, 0, 1, kw);
    }
    return kw[0] != Qundef && RTEST(kw[0]);
}

/* session.transport_events(depth = 16, history: false) -> EventListener:
 * a TransportEvent (kind :added / :removed, zid, whatami, qos, multicast)
 * each time a peer or router connects or goes. history: true reports the
 * ones connected now first. */
static VALUE zrb_session_transport_events(int argc, VALUE *argv, VALUE self) {
    VALUE depth_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "01:", &depth_v, &opts);
    bool history = zrb_history_kw(opts);
    zrb_session *z = zrb_session_get_open(self);
    zrb_lst *l;
    VALUE obj = zrb_lst_new(cEventListener, ZLST_TRANSPORT, ZEV_TRANSPORT, depth_v, &l);
    z_owned_closure_transport_event_t cb;
    z_closure_transport_event(&cb, zev_on_transport_event, zevq_on_drop, zevq_ref(l->q));
    z_transport_events_listener_options_t to;
    z_transport_events_listener_options_default(&to);
    to.history = history;
    z_result_t r = z_declare_transport_events_listener(z_loan(z->session), &l->u.t, z_move(cb), &to);
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot declare a transport events listener (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    zrb_lst_linked(obj, l, z, self);
    return obj;
}

/* session.link_events(depth = 16, history: false) -> EventListener: a
 * LinkEvent (kind, zid, src, dst, mtu, streamed, reliability, interfaces,
 * group, auth_identifier) each time a link opens or closes. */
static VALUE zrb_session_link_events(int argc, VALUE *argv, VALUE self) {
    VALUE depth_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "01:", &depth_v, &opts);
    bool history = zrb_history_kw(opts);
    zrb_session *z = zrb_session_get_open(self);
    zrb_lst *l;
    VALUE obj = zrb_lst_new(cEventListener, ZLST_LINK, ZEV_LINK, depth_v, &l);
    z_owned_closure_link_event_t cb;
    z_closure_link_event(&cb, zev_on_link_event, zevq_on_drop, zevq_ref(l->q));
    z_link_events_listener_options_t lo;
    z_link_events_listener_options_default(&lo);
    lo.history = history;
    z_result_t r = z_declare_link_events_listener(z_loan(z->session), &l->u.l, z_move(cb), &lo);
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot declare a link events listener (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    zrb_lst_linked(obj, l, z, self);
    return obj;
}

/* ---------------------------------------------------- advanced subscriber */

static const char *const zrb_history_keys[] = {"max_samples", "max_age", "detect_late_publishers"};
static const char *const zrb_recovery_keys[] = {"periodic_queries_ms", "heartbeat"};

/* session.advanced_subscriber(key, depth = 16, history: nil, recovery: nil,
 *                             subscriber_detection: false, query_timeout_ms: nil) -> AdvancedSubscriber
 * history: true or {max_samples:, max_age: (seconds), detect_late_publishers:}
 * asks the advanced publishers' caches for what they kept (late joiners get
 * the last values; ROS 2's transient local). recovery: true or
 * {periodic_queries_ms:} (or {heartbeat: true}) asks again for samples
 * that were missed. The rest is the same as subscribe. */
static VALUE zrb_session_advanced_subscriber(int argc, VALUE *argv, VALUE self) {
    VALUE key_v, depth_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "11:", &key_v, &depth_v, &opts);
    VALUE kw[4] = {Qundef, Qundef, Qundef, Qundef};
    if (!NIL_P(opts)) {
        ID ids[4] = {id_history, rb_intern("recovery"), rb_intern("subscriber_detection"),
                     rb_intern("query_timeout_ms")};
        rb_get_kwargs(opts, ids, 0, 4, kw);
    }
    long depth = zrb_check_depth(depth_v);
    ze_advanced_subscriber_options_t ao;
    ze_advanced_subscriber_options_default(&ao);
    ao.subscriber_options.allowed_origin = Z_LOCALITY_REMOTE;
    VALUE hist = kw[0] == Qundef ? Qnil : kw[0];
    VALUE rec = kw[1] == Qundef ? Qnil : kw[1];
    if (RTEST(hist)) {
        VALUE h = zrb_subopt_hash(hist, "history", zrb_history_keys, 3);
        if (NIL_P(h) && hist != Qtrue) {
            rb_raise(rb_eTypeError, "history must be true or a Hash");
        }
        ao.history.is_enabled = true;
        VALUE v;
        if (!NIL_P(v = zrb_hget(h, "max_samples"))) {
            ao.history.max_samples = (size_t)NUM2ULONG(v);
        }
        if (!NIL_P(v = zrb_hget(h, "max_age"))) {
            double s = NUM2DBL(v);
            if (s < 0) {
                rb_raise(rb_eArgError, "max_age must be 0 or more");
            }
            ao.history.max_age_ms = (uint64_t)(s * 1000.0);
        }
        if (!NIL_P(v = zrb_hget(h, "detect_late_publishers"))) {
            ao.history.detect_late_publishers = RTEST(v);
        }
    }
    if (RTEST(rec)) {
        VALUE h = zrb_subopt_hash(rec, "recovery", zrb_recovery_keys, 2);
        if (NIL_P(h) && rec != Qtrue) {
            rb_raise(rb_eTypeError, "recovery must be true or a Hash");
        }
        ao.recovery.is_enabled = true;
        VALUE v;
        if (!NIL_P(v = zrb_hget(h, "periodic_queries_ms"))) {
            ao.recovery.last_sample_miss_detection.is_enabled = true;
            ao.recovery.last_sample_miss_detection.periodic_queries_period_ms = zrb_ms_arg(v, "periodic_queries_ms");
        } else if (RTEST(zrb_hget(h, "heartbeat"))) {
            ao.recovery.last_sample_miss_detection.is_enabled = true;
            ao.recovery.last_sample_miss_detection.periodic_queries_period_ms = 0;
        }
    }
    if (kw[2] != Qundef) {
        ao.subscriber_detection = RTEST(kw[2]);
    }
    if (kw[3] != Qundef && !NIL_P(kw[3])) {
        ao.query_timeout_ms = zrb_ms_arg(kw[3], "query_timeout_ms");
    }
    VALUE key_str = zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t view;
    const z_loaned_keyexpr_t *ke = zrb_key_loan(key_v, &view);

    zrb_sub *s;
    VALUE obj = TypedData_Make_Struct(cAdvSubscriber, zrb_sub, &zrb_sub_type, s);
    s->type = ZSUB_ADVANCED;
    s->ch = zch_new(ZCH_SAMPLE, (uint32_t)depth);
    z_owned_closure_sample_t cb;
    zch_closure_sample(s->ch, &cb);
    z_result_t ret = ze_declare_advanced_subscriber(z_loan(z->session), &s->u.adv, ke, z_move(cb), &ao);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot declare an advanced subscriber on %" PRIsVALUE " (%d)%" PRIsVALUE, key_str, (int)ret, zrb_last_error());
    }
    s->declared = true;
    s->owner = z;
    s->next = z->subs;
    z->subs = s;
    zrb_hold_session(obj, self, key_str);
    return obj;
}

static zrb_sub *zrb_advsub_get_open(VALUE self) {
    zrb_sub *s = zrb_sub_get(self);
    if (s->type != ZSUB_ADVANCED || !s->declared) {
        rb_raise(eZenohError, "the subscriber is closed");
    }
    if (zrb_session_check_link(s->owner)) {
        rb_raise(eZenohError, "session is closed");
    }
    return s;
}

/* advsub.miss_listener(depth = 16) -> EventListener of Miss (source_zid,
 * source_eid, count): samples a publisher sent that never came (needs
 * sample_miss_detection on the advanced publisher). */
static VALUE zrb_advsub_miss_listener(int argc, VALUE *argv, VALUE self) {
    VALUE depth_v = Qnil;
    rb_scan_args(argc, argv, "01", &depth_v);
    zrb_sub *s = zrb_advsub_get_open(self);
    zrb_lst *l;
    VALUE obj = zrb_lst_new(cEventListener, ZLST_MISS, ZEV_MISS, depth_v, &l);
    ze_owned_closure_miss_t cb;
    ze_closure_miss(&cb, zev_on_miss, zevq_on_drop, zevq_ref(l->q));
    z_result_t r = ze_advanced_subscriber_declare_sample_miss_listener(z_loan(s->u.adv), &l->u.s, z_move(cb));
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot declare a sample miss listener (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    zrb_lst_linked(obj, l, s->owner, self);
    return obj;
}

/* advsub.detect_publishers(depth = 16, history: true) -> LivelinessWatch:
 * each_pending { |key, alive| } for the advanced publishers on the key
 * (those declared with publisher_detection: true). */
static VALUE zrb_advsub_detect_publishers(int argc, VALUE *argv, VALUE self) {
    VALUE depth_v = Qnil, opts = Qnil;
    rb_scan_args(argc, argv, "01:", &depth_v, &opts);
    VALUE kw[1] = {Qundef};
    if (!NIL_P(opts)) {
        rb_get_kwargs(opts, &id_history, 0, 1, kw);
    }
    bool history = (kw[0] == Qundef) ? true : RTEST(kw[0]);
    long depth = zrb_check_depth(depth_v);
    zrb_sub *as = zrb_advsub_get_open(self);
    zrb_sub *s;
    VALUE obj = TypedData_Make_Struct(cWatch, zrb_sub, &zrb_sub_type, s);
    s->liveliness = true;
    s->type = ZSUB_LIVELINESS;
    s->ch = zch_new(ZCH_SAMPLE, (uint32_t)depth);
    z_owned_closure_sample_t cb;
    zch_closure_sample(s->ch, &cb);
    z_liveliness_subscriber_options_t lo;
    z_liveliness_subscriber_options_default(&lo);
    lo.history = history;
    z_result_t ret = ze_advanced_subscriber_detect_publishers(z_loan(as->u.adv), &s->u.sub, z_move(cb), &lo);
    if (ret != Z_OK) {
        rb_raise(eZenohError, "cannot detect publishers (%d)%" PRIsVALUE, (int)ret, zrb_last_error());
    }
    s->declared = true;
    s->owner = as->owner;
    s->next = as->owner->subs;
    as->owner->subs = s;
    rb_ivar_set(obj, id_iv_session, rb_ivar_get(self, id_iv_session));
    rb_ivar_set(obj, id_iv_parent, self);
    return obj;
}

/* ------------------------------------------------- session information */

/* Everything queued, as an Array (a snapshot's queue). */
static VALUE zevq_each_pending_noblock(zevq *q, bool event) {
    VALUE out = rb_ary_new();
    zev *e;
    while ((e = zevq_take(q)) != NULL) {
        VALUE v = zev_to_ruby(q->kind, e, event);
        zev_free(e);
        rb_ary_push(out, v);
    }
    return out;
}

typedef struct {
    pthread_mutex_t lock;
    z_id_t *ids;
    size_t n, cap;
} zrb_zids;

static void zrb_collect_zid(const z_id_t *id, void *ctx) {
    zrb_zids *c = (zrb_zids *)ctx;
    pthread_mutex_lock(&c->lock);
    if (c->n == c->cap) {
        size_t cap = c->cap ? c->cap * 2 : 8;
        z_id_t *ids = (z_id_t *)realloc(c->ids, cap * sizeof(z_id_t));
        if (ids == NULL) {
            pthread_mutex_unlock(&c->lock);
            return;
        }
        c->ids = ids;
        c->cap = cap;
    }
    c->ids[c->n++] = *id;
    pthread_mutex_unlock(&c->lock);
}

static VALUE zrb_session_zids(VALUE self, bool routers) {
    zrb_session *z = zrb_session_get(self);
    if (zrb_session_check_link(z)) {
        return rb_ary_new();
    }
    zrb_zids c;
    memset(&c, 0, sizeof(c));
    pthread_mutex_init(&c.lock, NULL);
    z_owned_closure_zid_t cb;
    z_closure_zid(&cb, zrb_collect_zid, NULL, &c);
    if (routers) {
        z_info_routers_zid(z_loan(z->session), z_move(cb));
    } else {
        z_info_peers_zid(z_loan(z->session), z_move(cb));
    }
    VALUE out = rb_ary_new_capa((long)c.n);
    for (size_t i = 0; i < c.n; i++) {
        rb_ary_push(out, zrb_zid_value(&c.ids[i]));
    }
    free(c.ids);
    pthread_mutex_destroy(&c.lock);
    return out;
}

/* session.peer_zids / router_zids -> Array of String: the Zenoh IDs of the
 * peers / routers connected now (session.peers stays the count). */
static VALUE zrb_session_peer_zids(VALUE self) { return zrb_session_zids(self, false); }
static VALUE zrb_session_router_zids(VALUE self) { return zrb_session_zids(self, true); }

/* session.transports -> Array of Transport (zid, whatami, qos, multicast) */
static VALUE zrb_session_transports(VALUE self) {
    zrb_session *z = zrb_session_get_open(self);
    zevq *q = zevq_new(ZEV_TRANSPORT, ZRB_MAX_DEPTH);
    z_owned_closure_transport_t cb;
    z_closure_transport(&cb, zev_on_transport, zevq_on_drop, zevq_ref(q));
    z_info_transports(z_loan(z->session), z_move(cb));
    VALUE out = zevq_each_pending_noblock(q, false);
    zevq_unref(q);
    return out;
}

/* session.links -> Array of Link (zid, src, dst, mtu, streamed, ...) */
static VALUE zrb_session_links_m(VALUE self) {
    zrb_session *z = zrb_session_get_open(self);
    zevq *q = zevq_new(ZEV_LINK, ZRB_MAX_DEPTH);
    z_owned_closure_link_t cb;
    z_closure_link(&cb, zev_on_link, zevq_on_drop, zevq_ref(q));
    z_info_links_options_t lo;
    z_info_links_options_default(&lo);
    z_info_links(z_loan(z->session), z_move(cb), &lo);
    VALUE out = zevq_each_pending_noblock(q, false);
    zevq_unref(q);
    return out;
}

/* session.new_timestamp -> Timestamp (from the session's clock and ID) */
static VALUE zrb_session_new_timestamp(VALUE self) {
    zrb_session *z = zrb_session_get_open(self);
    z_timestamp_t ts;
    if (z_timestamp_new(&ts, z_loan(z->session)) != Z_OK) {
        rb_raise(eZenohError, "cannot make a timestamp");
    }
    return zrb_ts_wrap(&ts);
}

/* session.declare_keyexpr(key) -> KeyExpr, declared on the session: the
 * key goes over the wire as a short number afterwards. Pass it to put,
 * delete, publisher, querier and the advanced ones of the same session.
 * Undeclared by KeyExpr#undeclare or when the session closes. */
static VALUE zrb_session_declare_keyexpr(VALUE self, VALUE key_v) {
    zrb_key_string(key_v);
    zrb_session *z = zrb_session_get_open(self);
    z_view_keyexpr_t view;
    const z_loaned_keyexpr_t *ke = zrb_key_loan(key_v, &view);
    zrb_ke *k;
    VALUE obj = TypedData_Make_Struct(cKeyExpr, zrb_ke, &zrb_ke_type, k);
    z_keyexpr_clone(&k->ke, ke);
    z_internal_keyexpr_null(&k->decl);
    z_result_t r = z_declare_keyexpr(z_loan(z->session), &k->decl, ke);
    if (r != Z_OK) {
        rb_raise(eZenohError, "cannot declare the key expression (%d)%" PRIsVALUE, (int)r, zrb_last_error());
    }
    zrb_ent_link(&k->ent, z, ZENT_KEYEXPR, true, zrb_ke_undeclare);
    rb_ivar_set(obj, id_iv_session, self);
    return obj;
}

/* ------------------------------------------------------ scouting, logs */

typedef struct {
    z_owned_config_t config;
    z_owned_closure_hello_t cb;
    z_scout_options_t opts;
    z_result_t ret;
} zrb_scout_args;

static void *zrb_scout_nogvl(void *p) {
    zrb_scout_args *a = (zrb_scout_args *)p;
    a->ret = z_scout(z_move(a->config), z_move(a->cb), &a->opts);
    return NULL;
}

/* Asterism::Zenoh._scout(what, timeout_ms, config) -> Array of Hello.
 * what: the bits of z_what_t (1 router, 2 peer, 4 client). config: nil or
 * a Hash as for Session.open. Waits timeout_ms (without the GVL). The
 * Ruby wrapper is Asterism::Zenoh.scout. */
static VALUE zrb_s_scout(VALUE mod, VALUE what_v, VALUE timeout_v, VALUE config_v) {
    int what = NUM2INT(what_v);
    if (what < 1 || what > 7) {
        rb_raise(rb_eArgError, "what must be 1..7");
    }
    long timeout = zrb_check_timeout(timeout_v);
    if (!NIL_P(config_v)) {
        Check_Type(config_v, T_HASH);
        zrb_to_json(Qnil);
    }
    zrb_scout_args a;
    if (z_config_default(&a.config) != Z_OK) {
        rb_raise(eZenohError, "cannot create the configuration");
    }
    zrb_config_set(&a.config, "scouting/multicast/enabled", "true");
    if (!NIL_P(config_v)) {
        zrb_cfg_hash h = {config_v, &a.config, Qnil};
        int state = 0;
        rb_protect(zrb_cfg_hash_body, (VALUE)&h, &state);
        if (state != 0 || !NIL_P(h.bad)) {
            z_drop(z_move(a.config));
            if (state != 0) {
                rb_jump_tag(state);
            }
            rb_raise(rb_eArgError, "invalid configuration: %" PRIsVALUE, h.bad);
        }
    }
    zevq *q = zevq_new(ZEV_HELLO, ZRB_MAX_DEPTH);
    z_closure_hello(&a.cb, zev_on_hello, zevq_on_drop, zevq_ref(q));
    z_scout_options_default(&a.opts);
    a.opts.timeout_ms = (uint64_t)timeout;
    a.opts.what = (z_what_t)what;
    rb_thread_call_without_gvl(zrb_scout_nogvl, &a, RUBY_UBF_IO, NULL);
    VALUE out = zevq_each_pending_noblock(q, false);
    zevq_unref(q);
    if (a.ret != Z_OK) {
        rb_raise(eZenohError, "scouting failed (%d)", (int)a.ret);
    }
    return out;
}

/* Asterism::Zenoh.init_log(level = nil) -> nil: zenoh-c's log, written to
 * standard output (zenoh-c's own logger).
 * level: "error", "warn", "info", "debug", "trace" (or a filter such as
 * "zenoh::net=debug"); RUST_LOG, when set, wins. nil: only RUST_LOG. Once
 * per process: later calls change nothing. */
static VALUE zrb_s_init_log(int argc, VALUE *argv, VALUE mod) {
    VALUE level = Qnil;
    rb_scan_args(argc, argv, "01", &level);
    if (NIL_P(level)) {
        zc_try_init_log_from_env();
        return Qnil;
    }
    VALUE s = rb_obj_as_string(level);
    z_result_t r = zc_init_log_from_env_or(StringValueCStr(s));
    if (r != Z_OK) {
        rb_raise(rb_eArgError, "invalid log level: %" PRIsVALUE, s);
    }
    return Qnil;
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
    id_iv_parent = rb_intern("@parent");
    id_config = rb_intern("config");
    id_config_file = rb_intern("config_file");
    id_scouting = rb_intern("scouting");
    id_history = rb_intern("history");
    id_new = rb_intern("new");
    id_client = rb_intern("client");
    id_peer = rb_intern("peer");
    id_router = rb_intern("router");
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
    rb_define_method(cSession, "delete", zrb_session_delete, -1);
    rb_define_method(cSession, "publisher", zrb_session_publisher, -1);
    rb_define_method(cSession, "advanced_publisher", zrb_session_advanced_publisher, -1);
    rb_define_method(cSession, "advanced_subscriber", zrb_session_advanced_subscriber, -1);
    rb_define_method(cSession, "querier", zrb_session_querier, -1);
    rb_define_method(cSession, "transport_events", zrb_session_transport_events, -1);
    rb_define_method(cSession, "link_events", zrb_session_link_events, -1);
    rb_define_method(cSession, "peer_zids", zrb_session_peer_zids, 0);
    rb_define_method(cSession, "router_zids", zrb_session_router_zids, 0);
    rb_define_method(cSession, "transports", zrb_session_transports, 0);
    rb_define_method(cSession, "links", zrb_session_links_m, 0);
    rb_define_method(cSession, "new_timestamp", zrb_session_new_timestamp, 0);
    rb_define_method(cSession, "declare_keyexpr", zrb_session_declare_keyexpr, 1);
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
    rb_define_method(cSubscriber, "each_sample", zrb_sub_each_sample, 0);

    /* AdvancedSubscriber: a Subscriber with history, recovery, missed
     * samples and publisher detection. */
    cAdvSubscriber = rb_define_class_under(mZenoh, "AdvancedSubscriber", rb_cObject);
    zrb_no_new(cAdvSubscriber);
    rb_define_method(cAdvSubscriber, "each_pending", zrb_sub_each_pending, 0);
    rb_define_method(cAdvSubscriber, "each_sample", zrb_sub_each_sample, 0);
    rb_define_method(cAdvSubscriber, "pending", zrb_sub_pending, 0);
    rb_define_method(cAdvSubscriber, "received", zrb_sub_received, 0);
    rb_define_method(cAdvSubscriber, "dropped", zrb_sub_dropped, 0);
    rb_define_method(cAdvSubscriber, "close", zrb_sub_close, 0);
    rb_define_method(cAdvSubscriber, "closed?", zrb_sub_closed_p, 0);
    rb_define_method(cAdvSubscriber, "miss_listener", zrb_advsub_miss_listener, -1);
    rb_define_method(cAdvSubscriber, "detect_publishers", zrb_advsub_detect_publishers, -1);

    /* Publisher and AdvancedPublisher share one C structure. */
    cPublisher = rb_define_class_under(mZenoh, "Publisher", rb_cObject);
    cAdvPublisher = rb_define_class_under(mZenoh, "AdvancedPublisher", rb_cObject);
    VALUE pubs[2] = {cPublisher, cAdvPublisher};
    for (int i = 0; i < 2; i++) {
        zrb_no_new(pubs[i]);
        rb_define_method(pubs[i], "put", zrb_pub_put, -1);
        rb_define_method(pubs[i], "delete", zrb_pub_delete, -1);
        rb_define_method(pubs[i], "matching?", zrb_pub_matching_p, 0);
        rb_define_method(pubs[i], "matching_listener", zrb_pub_matching_listener, -1);
        rb_define_method(pubs[i], "close", zrb_pub_close, 0);
        rb_define_method(pubs[i], "closed?", zrb_pub_closed_p, 0);
    }

    cQuerier = rb_define_class_under(mZenoh, "Querier", rb_cObject);
    zrb_no_new(cQuerier);
    rb_define_method(cQuerier, "get", zrb_querier_get_m, -1);
    rb_define_method(cQuerier, "matching?", zrb_querier_matching_p, 0);
    rb_define_method(cQuerier, "matching_listener", zrb_querier_matching_listener, -1);
    rb_define_method(cQuerier, "close", zrb_querier_close, 0);
    rb_define_method(cQuerier, "closed?", zrb_querier_closed_p, 0);

    cMatchingListener = rb_define_class_under(mZenoh, "MatchingListener", rb_cObject);
    cEventListener = rb_define_class_under(mZenoh, "EventListener", rb_cObject);
    VALUE lsts[2] = {cMatchingListener, cEventListener};
    for (int i = 0; i < 2; i++) {
        zrb_no_new(lsts[i]);
        rb_define_method(lsts[i], "each_pending", zrb_lst_each_pending, 0);
        rb_define_method(lsts[i], "pending", zrb_lst_pending, 0);
        rb_define_method(lsts[i], "received", zrb_lst_received, 0);
        rb_define_method(lsts[i], "dropped", zrb_lst_dropped, 0);
        rb_define_method(lsts[i], "close", zrb_lst_close, 0);
        rb_define_method(lsts[i], "closed?", zrb_lst_closed_p, 0);
    }

    cKeyExpr = rb_define_class_under(mZenoh, "KeyExpr", rb_cObject);
    rb_undef_alloc_func(cKeyExpr);
    rb_define_singleton_method(cKeyExpr, "new", zrb_ke_s_new, -1);
    rb_define_singleton_method(cKeyExpr, "canonize", zrb_ke_s_canonize, 1);
    rb_define_singleton_method(cKeyExpr, "valid?", zrb_ke_s_valid_p, 1);
    rb_define_method(cKeyExpr, "to_s", zrb_ke_to_s, 0);
    rb_define_method(cKeyExpr, "to_str", zrb_ke_to_s, 0);
    rb_define_method(cKeyExpr, "inspect", zrb_ke_inspect, 0);
    rb_define_method(cKeyExpr, "intersects?", zrb_ke_intersects_p, 1);
    rb_define_method(cKeyExpr, "includes?", zrb_ke_includes_p, 1);
    rb_define_method(cKeyExpr, "relation_to", zrb_ke_relation_to, 1);
    rb_define_method(cKeyExpr, "join", zrb_ke_join, 1);
    rb_define_method(cKeyExpr, "concat", zrb_ke_concat, 1);
    rb_define_method(cKeyExpr, "==", zrb_ke_eq, 1);
    rb_define_method(cKeyExpr, "eql?", zrb_ke_eq, 1);
    rb_define_method(cKeyExpr, "hash", zrb_ke_hash, 0);
    rb_define_method(cKeyExpr, "declared?", zrb_ke_declared_p, 0);
    rb_define_method(cKeyExpr, "undeclare", zrb_ke_undeclare_m, 0);

    cTimestamp = rb_define_class_under(mZenoh, "Timestamp", rb_cObject);
    zrb_no_new(cTimestamp);
    rb_include_module(cTimestamp, rb_mComparable);
    rb_define_method(cTimestamp, "ntp64", zrb_ts_ntp64, 0);
    rb_define_method(cTimestamp, "id", zrb_ts_id, 0);
    rb_define_method(cTimestamp, "to_time", zrb_ts_to_time, 0);
    rb_define_method(cTimestamp, "<=>", zrb_ts_cmp, 1);
    rb_define_method(cTimestamp, "==", zrb_ts_eql, 1);
    rb_define_method(cTimestamp, "eql?", zrb_ts_eql, 1);
    rb_define_method(cTimestamp, "hash", zrb_ts_hash, 0);
    rb_define_method(cTimestamp, "to_s", zrb_ts_to_s, 0);
    rb_define_method(cTimestamp, "inspect", zrb_ts_inspect, 0);

    rb_define_module_function(mZenoh, "_scout", zrb_s_scout, 3);
    rb_define_module_function(mZenoh, "init_log", zrb_s_init_log, -1);

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
    rb_define_method(cQuery, "reply_err", zrb_query_reply_err, -1);
    rb_define_method(cQuery, "reply_del", zrb_query_reply_del, -1);
    rb_define_method(cQuery, "encoding", zrb_query_encoding, 0);
    rb_define_method(cQuery, "finish", zrb_query_finish_m, 0);
    rb_define_method(cQuery, "finished?", zrb_query_finished_p, 0);

    cGet = rb_define_class_under(mZenoh, "Get", rb_cObject);
    zrb_no_new(cGet);
    rb_define_method(cGet, "each_reply", zrb_get_each_reply, 0);
    rb_define_method(cGet, "each_result", zrb_get_each_result, 0);
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
