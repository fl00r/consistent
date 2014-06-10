#include "ruby/ruby.h"

#include <openssl/md5.h>

#define CONSISTENT_IMPLEMENTATION
#include "consistent.h"

typedef ConsistentHash_t * MR__Consistent;

static void md5_points_hash(__unused__ void *_ctx, const char *serv, size_t len, uint32_t seed, uint32_t digest[4]) {   
    MD5_CTX ctx;
    uint64_t seedb = seed;
    MD5_Init(&ctx);
    MD5_Update(&ctx, (unsigned char*)&seedb, sizeof(seedb));
    MD5_Update(&ctx, (unsigned char*)serv, len);
    MD5_Final((unsigned char*)digest, &ctx);
}




// Ruby methods
VALUE Consistent = Qnil;

VALUE method_get(VALUE self, VALUE token, VALUE cnt, VALUE all);
VALUE method_add(VALUE self, VALUE items);
VALUE method_update(VALUE self, VALUE items);
// VALUE method_replace(VALUE self, VALUE items);

CH_config_t config = {
    .use_handle = CH_DONOT_USE_HANDLE,
    .points_hash = md5_points_hash,
    .points_per_server = 500
};

ConsistentHash_t* get_Ring(VALUE self) {
  ConsistentHash_t* ring;
  Data_Get_Struct(self, ConsistentHash_t, ring);
  return ring;
}

void wrap_Ring(VALUE klass) {
  ConsistentHash_t* ring = ConsistentHash_new(config);
  Data_Wrap_Struct(klass, NULL, ConsistentHash_free, ring);
}

void Init_consistent_ring() {
  Consistent = rb_define_class("ConsistentRing", rb_cObject);
  wrap_Ring(Consistent);

  rb_define_alloc_func(Consistent, wrap_Ring);
  // rb_define_method(Consistent, "initialize", method_init, 0);
  rb_define_method(Consistent, "get", method_get, 3);
  rb_define_method(Consistent, "add", method_add, 1);
  rb_define_method(Consistent, "update", method_update, 1);
  // rb_define_method(Consistent, "replace", method_replace, 1);
}

VALUE method_get(VALUE self, VALUE token_r, VALUE cnt_r, VALUE all_r) {
  ConsistentHash_t *ring;
  ring = get_Ring(self);
  char *token = RSTRING_PTR(token_r);
  int token_len = RSTRING_LEN(token_r);
  int i;
  VALUE nodes = rb_ary_new();
  ConsistentHash_Iterator_t *iter = ConsistentHash_Iterator_new(ring, token, token_len);
  if(all_r == Qnil){
    int cnt = NUM2INT(cnt_r);
    for(i = 0; i < cnt; i++) {
      ConsistentHash_IteratorName_t res = ConsistentHash_Iterator_next_name(iter);
      if(res.name != NULL) {
        rb_ary_push(nodes, rb_str_new2(res.name));
      } else {
        break;
      }
    }
  }else{
    for(i = 0; ; i++) {
      ConsistentHash_IteratorName_t res = ConsistentHash_Iterator_next_name(iter);
      if(res.name != NULL) {
        rb_ary_push(nodes, rb_str_new2(res.name));
      } else {
        break;
      }
    }
  }
  ConsistentHash_Iterator_free(iter);

  return nodes;
}

VALUE method_add(VALUE self, VALUE items) {
  ConsistentHash_t *ring;
  ring = get_Ring(self);
  ConsistentHash_ServerList_t *list = ConsistentHash_ServerList_new(ring);
  long items_size = RARRAY_LEN(items);
  VALUE node_str = rb_str_new2("node");
  VALUE weight_str = rb_str_new2("weight");
  VALUE status_str = rb_str_new2("status");
  int i;

  for(i = 0; i < items_size; i++){
    VALUE item = rb_ary_entry(items, i);
    VALUE node = rb_hash_aref(item, node_str);
    char *name = RSTRING_PTR(node);
    size_t name_len = RSTRING_LEN(node);
    uint32_t weight = NUM2INT(rb_hash_aref(item, weight_str));
    uint32_t alive = NUM2INT(rb_hash_aref(item, status_str));
    ConsistentHash_ServerList_add(list, name, name_len, weight, alive, 0);
  }
  ConsistentHash_exchange_server_list(ring, list);
  ConsistentHash_ServerList_free(list);
  return Qnil;
}


VALUE method_update(VALUE self, VALUE items) {
  ConsistentHash_t *ring = get_Ring(self);
  ConsistentHash_AliveByName_t *list = ConsistentHash_AliveByName_new(ring);

  long items_size = RARRAY_LEN(items);
  VALUE node_str = rb_str_new2("node");
  VALUE status_str = rb_str_new2("status");
  int  i;

  for(i = 0; i < items_size; i++){
    VALUE item = rb_ary_entry(items, i);
    VALUE node = rb_hash_aref(item, node_str);
    char *name = RSTRING_PTR(node);
    size_t name_len = RSTRING_LEN(node);
    VALUE alive = NUM2INT(rb_hash_aref(item, status_str));

    ConsistentHash_AliveByName_add(list, name, name_len, alive);
  }
  CH_aliveness_e default_alive = CH_DEFAULT;
  ConsistentHash_refresh_alive_by_name(ring, list, default_alive);
  ConsistentHash_AliveByName_free(list);
  return Qnil;
}
