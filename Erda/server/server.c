#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <libpmem.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <pthread.h>
#include "DS.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define MSG_SIZE (sizeof(struct Message))
//#define MSG_SIZE (sizeof(struct Message)+sizeof(struct IDBucket_))

#define BUFFER_SIZE 8192


hmap H1,H2;




enum{
  PUT,
  GET,
  INSETW,
  RHEAD1,//PUT
  RHEAD2,//GET
  GETHT,
  GETHT2,
  H1ADDR,
  H2ADDR,
  REGHEAD,
  REGHEADOK,
  REGRBUF,
  REGRBUFOK,
  REGCBUF,
  REGCBUFOK,
  RMR,
  MSG_MR,
  WS
};
enum SERVERSTATE {
  ESTABLISHED = 1,
  METADATARECV,
  MRSEND,
  LOGFINISHED,
  CLEANLOG,
  //RECOVERY 
} ;

struct timeval start,end,end1;
void settime(){
    gettimeofday( &start, NULL );
}

void gettime(){
    gettimeofday( &end, NULL );
}

void gettime1(){
    gettimeofday( &end1, NULL );
}


/*void printall(){
    int timeuse;
    timeuse = 1000000 * ( end.tv_sec - start.tv_sec ) + end.tv_usec -start.tv_usec;
    printf("time: %d us\n", timeuse);
    timeuse = 1000000 * ( end1.tv_sec - start.tv_sec ) + end1.tv_usec -start.tv_usec;
    printf("time: %d us\n", timeuse);
}*/



struct Message{
  uint8_t type;
  char key[32];
  //uint32_t datasize;
  struct ibv_mr mr;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *write_mr;
  struct ibv_mr *write_metamr;
  struct ibv_mr *write_metamr2;
  struct ibv_mr *mbuffer_mr;

  struct ibv_mr *mbuffer;
  char *write_region;
  char *recv_region;
  char *send_region;
  struct Bucket_ *write_metaregion;
  struct Bucket_ *write_metaregion2;

  int state;
};

struct ibv_mr cbuf_mr;

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

int send_msg(struct connection * conn, struct Message *msg);
void sendmr(struct connection *conn);
void register_datamemory(struct connection *conn, int len,const char *);
void register_metadatamemory(struct connection *conn, int len, const struct Bucket_ *addr, const struct Bucket_ *addr2);
void write_log(char *filename, int len, struct connection * conn);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
#if _USE_IPV6
  struct sockaddr_in6 addr;
#else
  struct sockaddr_in addr;
#endif
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;

  memset(&addr, 0, sizeof(addr));
#if _USE_IPV6
  addr.sin6_family = AF_INET6;
#else
  addr.sin_family = AF_INET;
#endif

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  struct sockaddr_in * addr_m = (struct sockaddr_in*)rdma_get_local_addr(listener);
  // printf("addr:%s\n",inet_ntoa(addr_m->sin_addr));
  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);
  printf("pid: %d\n", getpid());

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}


void headinit(){
  HT = calloc(HASHCapacity,sizeof(struct Bucket_));
  HTT = calloc(HASHCapacity/MAX_SEARCH_RANGE,sizeof(struct Bucket_));
  //memset(HT,0,HASHCapacity*sizeof(struct Bucket_));
  //memset(HTT,0,HASHCapacity/3*sizeof(struct Bucket_));

  H1 = malloc(sizeof(struct hashmap));
  H1->size = 0;
  H1->capacity = HASHCapacity;
  H1->array = HT;
  H2 = malloc(sizeof(struct hashmap));
  H2->size = 0;
  H2->capacity = HASHCapacity/MAX_SEARCH_RANGE;
  H2->array = HTT;


  Segment seg1 = (Segment)malloc(sizeof(struct Segment_));
  memset(seg1->data,0,sizeof(HEADSIZE));
  /*Segment seg2 = (Segment)malloc(sizeof(struct Segment_));
  memset(seg2->data,0,sizeof(HEADSIZE));
  Segment seg3 = (Segment)malloc(sizeof(struct Segment_));
  memset(seg3->data,0,sizeof(HEADSIZE));
  Segment seg4 = (Segment)malloc(sizeof(struct Segment_));
  memset(seg4->data,0,sizeof(HEADSIZE));
  Segment seg5 = (Segment)malloc(sizeof(struct Segment_));
  memset(seg5->data,0,sizeof(HEADSIZE));
  Segment seg6 = (Segment)malloc(sizeof(struct Segment_));
  memset(seg6->data,0,sizeof(HEADSIZE));*/
 
  *HR = calloc(128, sizeof(Head));
  HR[0]->head_id = 0;
  HR[0]->sroot = seg1;
  HR[0]->count = 0;
  /*HR[1]->head_id = 0;
  HR[1]->sroot = seg2;
  HR[1]->count = 0;
  HR[2]->head_id = 0;
  HR[2]->sroot = seg3;
  HR[2]->count = 0;
  HR[3]->head_id = 0;
  HR[3]->sroot = seg4;
  HR[3]->count = 0;
  HR[4]->head_id = 0;
  HR[4]->sroot = seg5;
  HR[4]->count = 0;
  HR[5]->head_id = 0;
  HR[5]->sroot = seg6;
  HR[5]->count = 0;*/
}

/*IDBucket findkey(char * a){
  //int len = strlen(a);
  //uint32_t tid = murmurhash(a,len,SEED);
  //tid = 1;
  IDBucket t =  findHT(H1,H2,a);
  return t;
};*/

void addHead(struct connection *conn, IDBucket ht){
  memcpy((conn->send_region)+sizeof(struct Message), ht, sizeof(struct IDBucket_));
}

void addBucket(struct connection *conn, Bucket ht){
  memcpy((conn->send_region)+sizeof(struct Message), ht, sizeof(struct Bucket_));
}

void die(const char *reason)
{
  // fprintf(stderr, "%s\n", reason);
  perror("ERROR:");
  exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 16000, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 16000;
  qp_attr->cap.max_recv_wr = 16000;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
  qp_attr->sq_sig_all = 0;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;
  printf("poll_cq\n");
  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));
    //printf("1\n");
    while (ibv_poll_cq(cq, 1, &wc)){
      on_completion(&wc);
      //printf("end completion\n");
    }
  }

  return NULL;
}

int send_msg(struct connection * conn, struct Message *msg)
{
  struct ibv_send_wr wr,*bad_wr = NULL;
  struct ibv_sge sge;
  memcpy(conn->send_region,msg, sizeof(struct Message));
  //conn->send_region[sizeof(struct Message)] = '0';
  //printf("send %s",conn->send_region);
  memset(&wr, 0, sizeof(wr));
  // printf("send_msg\n");
  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  sge.lkey = conn->send_mr->lkey;
  sge.addr = (uintptr_t)conn->send_region;
  sge.length = MSG_SIZE;

  post_receives(conn);
  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

}

void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  // printf("post_recevies\n");
  memset(&wr,0,sizeof(wr));
  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_region;
  sge.length = MSG_SIZE;
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void post_receives_write(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  // printf("post_recv write\n");
  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = NULL;
  wr.num_sge = 0;
  TEST_NZ(ibv_post_recv(conn->qp,&wr,&bad_wr));
}

void write_with_imm(struct connection *conn){
  //printf("write with imm\n");
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  memset( &wr,0,sizeof(wr));
  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.imm_data = htonl(0);

  wr.wr.rdma.remote_addr = (uintptr_t)cbuf_mr.addr;
  wr.wr.rdma.rkey = cbuf_mr.rkey;

  //if(strlen(conn->keybuffer)){
      wr.sg_list = &sge;
      wr.num_sge = 1;
      sge.addr = (uintptr_t)conn->mbuffer;
      sge.length = sizeof(struct ibv_mr);
      sge.lkey = conn->mbuffer_mr->lkey;
    //} 
  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
  post_receives(conn);

}

void register_datamemory(struct connection *conn, int len, const char* addr){
  
  conn->write_region = (char *)addr;
  //printf("%x\n",conn->write_region);
  TEST_Z(conn->write_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->write_region,
    len,
    IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));
}

void register_metadatamemory(struct connection *conn, int len, const struct Bucket_ *addr, const struct Bucket_ *addr2){
  conn->write_metaregion = (struct Bucket_ *)addr;
  conn->write_metaregion2 = (struct Bucket_ *)addr2;

  TEST_Z(conn->write_metamr = ibv_reg_mr(
    s_ctx->pd,
    conn->write_metaregion,
    len, //byte
    IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));

  TEST_Z(conn->write_metamr2 = ibv_reg_mr(
    s_ctx->pd,
    conn->write_metaregion2,
    len/MAX_SEARCH_RANGE,
    IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));
}

void sendmr(struct connection *conn){
  // printf("sendmr\n");
    struct Message* msg = (struct Message *)malloc(sizeof(struct Message));

    msg->type = MSG_MR;
    //memcpy(&(msg->data.mr),conn->write_mr,sizeof(struct ibv_mr));
    //sleep(1);
    // printf("MR:  %x,%x\n",msg->data.mr.rkey,msg->data.mr.addr);
    send_msg(conn, msg);
    post_receives(conn);
    free(msg);
}



void register_memory(struct connection *conn)
{
  conn->send_region = malloc(MSG_SIZE);
  conn->recv_region = malloc(MSG_SIZE);
  conn->mbuffer = malloc(sizeof(struct ibv_mr));

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->send_region,
    MSG_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->recv_region,
    MSG_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->mbuffer_mr = ibv_reg_mr(
   s_ctx->pd,
   conn->mbuffer,
   sizeof(struct ibv_mr),
   IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));
}

char keybuf[32];
void on_completion(struct ibv_wc *wc)
{
  struct connection *conn = (struct connection *)wc->wr_id;
  //printf("wc->status:%d\n",wc->status);
  //printf("start completion\n");
  if (wc->status == IBV_WC_SUCCESS){
    if (wc->opcode & IBV_WC_RECV){
      struct Message msg_tmp ;
      //printf("recv MSG\n");
      memcpy(&msg_tmp, conn->recv_region,sizeof(struct Message));
      if(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM){
        uint32_t size = ntohl (wc->imm_data);
        if (size == 0){
         
         uint8_t ht = findHE(H1,H2,keybuf);
         //printf("Client Request -- PUT:%s\n",keybuf);
         if(ht!=128){
                             
          conn->write_mr->addr = HR[ht]->sroot->data + (HR[ht]->count)*xvs;
          memcpy((void *)(conn->mbuffer),(void *)(conn->write_mr),sizeof(struct ibv_mr));
          write_with_imm(conn);
          

          (HR[ht]->count)++;
          uint64_t offset;
          if(nudx == 0){
            offset = (uintptr_t)((HR[ht]->count-1)*xvs);
            setnewtag_newoffset(HT[indx].mark, getnewtag(HT[indx].mark), offset);
          }
          else{
            offset = (uintptr_t)((HR[ht]->count-1)*xvs);
            setnewtag_newoffset(HTT[indx].mark, getnewtag(HTT[indx].mark), offset);
          }
          
         }
         else{
        
          
          conn->write_mr->addr = HR[0]->sroot->data + (HR[0]->count)*xvs;
          memcpy((void *)(conn->mbuffer),(void *)(conn->write_mr),sizeof(struct ibv_mr));
          write_with_imm(conn);
          //printf("imm\n");
         
          uint8_t nht = setkey(H1,H2,keybuf,0);
          if(nht!=128){
            

            (HR[0]->count)++;            
         
          }
          else{
            //printf("not found bucket\n");
            //msg.type = INSETW;
            //send_msg(conn, &msg);
          }
         }
        }
        else{}
        //printf("recv mr\n");
        
      }

      

      else if(msg_tmp.type == GETHT){
        printf("Client Request -- GETHT:1\n");
        struct Message msg;
        msg.type = H1ADDR;
        register_metadatamemory(conn,HASHCapacity*sizeof(struct Bucket_),H1->array,H2->array);
        memcpy((void *)&(msg.mr),(void *)(conn->write_metamr),sizeof(struct ibv_mr));
        send_msg(conn, &msg);
      }

      else if(msg_tmp.type == GETHT2){
        printf("Client Request -- GETHT:2\n");
        struct Message msg;
        msg.type = H2ADDR;
        //register_metadatamemory(conn,HASHCapacity*sizeof(struct Bucket_),H1->array,H2->array);
        memcpy((void *)&(msg.mr),(void *)(conn->write_metamr2),sizeof(struct ibv_mr));
        send_msg(conn, &msg);
      }

      else if(msg_tmp.type == REGHEAD){
        printf("Client Request -- REGHEAD\n");
        int i = atoi(msg_tmp.key);
        struct Message msg;
        msg.type = REGHEADOK;
        strcpy(msg.key,msg_tmp.key);
        register_datamemory(conn,HEADSIZE,HR[i]->sroot->data);
        memcpy((void *)&(msg.mr),(void *)(conn->write_mr),sizeof(struct ibv_mr));
        //int k;
        //for(k=1;k<128;k++)
          //register_datamemory(conn,HEADSIZE,&(HR[i]->sroot->data[k*HEADSIZE]));
        send_msg(conn, &msg);
      }

      else if(msg_tmp.type == REGRBUF){
        printf("Client Request -- REGRBUF\n");
        struct Message msg;
        msg.type = REGRBUFOK;
        register_datamemory(conn,32,keybuf);
        memcpy((void *)&(msg.mr),(void *)(conn->write_mr),sizeof(struct ibv_mr));
        send_msg(conn, &msg);
      }

      else if(msg_tmp.type == REGCBUFOK){
        printf("REGCBUFOK\n");
        memcpy((void*)(&(cbuf_mr)), (void *)(&(msg_tmp.mr)), sizeof(struct ibv_mr));
      }

      /*else if(msg_tmp.type == WS){
        printf("write success\n");
        //(%c:%c\n", HR->sroot->data[0], HR->sroot->data[1]);
        ibv_dereg_mr(conn->write_mr);
        post_receives(conn);
      }*/

      //else{
        //printf("msg_type：%d\n", msg_tmp.type);
      //}
    }
    //printf("opcode: %d\n",wc->opcode);
  }
  else{
    printf("fail:%d\n",wc->status);
  }
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

  printf("received connection request.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
  conn->state = ESTABLISHED;
  conn->qp = id->qp;

  register_memory(conn);
  headinit();
  post_receives(conn);
  //printf("post receive\n");
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.initiator_depth = cm_params.responder_resources =1;
  cm_params.rnr_retry_count = 7;
  
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

void reg_clientbuf(struct connection *conn){
  struct Message* msg = (struct Message *)malloc(sizeof(struct Message));
  msg->type = REGCBUF;
  send_msg(conn, msg); 
}

int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;
  int i;
  reg_clientbuf(conn);
  //int i;
  //for(i=0;i<100000;i++);
  //struct ibv_send_wr wr, *bad_wr = NULL;
  // struct ibv_sge sge;
  // printf("on connection\n");
  // memset(&wr, 0, sizeof(wr));
  // wr.opcode = IBV_WR_SEND;
  // wr.sg_list = &sge;
  // wr.num_sge = 1;
  // wr.send_flags = IBV_SEND_SIGNALED;
  // sge.addr = (uintptr_t)conn->send_region;
  // sge.length = MSG_SIZE;
  // sge.lkey = conn->send_mr->lkey;

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;
  printf("peer disconnected.\n");
  rdma_destroy_qp(id);
  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);
  free(conn->send_region);
  free(conn->recv_region);
  free(conn);
  rdma_destroy_id(id);
  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}
  
