#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <netdb.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <pthread.h>
#include "DS_c.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define BUFFER_SIZE 8192
#define MSG_SIZE (sizeof(struct Message)+sizeof(struct IDBucket_))

#define TIMEOUT_IN_MS 500


int compute = 0;
int wcount=0,rcount=0;

struct Transaction {
	enum ClientState {
		ESTABLISHED=1, 
		/**/SENDMETADATA, 
		/**/RECVMR,
		LOGRECV,
		WRITEDATA, 
		FINISH,
		FAIL, 
	} state;
};
enum MSG_ID{
		PUT,
		GET,
		WRITEBUFFEROK,
		READOK,
		REGRBUF,
		REGRBUFOK,
        INSETW,
		RHEAD1,//put
		RHEAD2,//get
		GETHT,
		GETHT2,
		H1ADDR,
        H2ADDR,
		RMR,
		MSG_MR,
		WS
};

struct Message{
	uint8_t type;
	char data[valuesize+20];
	uint32_t datasize;
	struct ibv_mr mr;
};

struct R_{
    char key[keysize];
    uint8_t hid;
    uintptr_t mark;
};

struct context{
	struct rdma_cm_id *id;
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_comp_channel *comp_channel;
	struct ibv_qp *qp;
	struct ibv_mr *recv_mr;
	struct ibv_mr *send_mr;
	struct ibv_mr remote_mr;
	struct ibv_mr *buffer_mr;
	struct ibv_mr *hbuffer_mr;
    struct ibv_mr *fbuffer_mr;

	struct R_ *fbuffer;
    char *buffer;
	uintptr_t *hbuffer;
	char *recv_buffer;
	char *send_buffer;
	int num_completions;
	struct Transaction tr;

	pthread_t poll_thread;
};


struct timespec start,end,end1,end2,end3,end4;

void settime(){
    clock_gettime(CLOCK_REALTIME, &start);
}

void gettime(){
    clock_gettime( CLOCK_REALTIME,&end);
}

void gettime1(){
    clock_gettime(CLOCK_REALTIME, &end1);
}
void gettime2(){
    clock_gettime(CLOCK_REALTIME, &end2);
}
void gettime3(){
    clock_gettime(CLOCK_REALTIME, &end3);
}
void gettime4(){
    clock_gettime(CLOCK_REALTIME, &end4);
}
void gettime5(){
    clock_gettime(CLOCK_REALTIME, &end5);
}
void gettime6(){
    clock_gettime(CLOCK_REALTIME, &end6);
}
void gettime7(){
    clock_gettime(CLOCK_REALTIME, &end7);
}
void gettime8(){
    clock_gettime(CLOCK_REALTIME, &end8);
}

/*
long long WTotalTime=0;
void compute_writelatency(){   
    WTotalTime = WTotalTime + ((1000000*end2.tv_sec+end2.tv_nsec/1000) - (1000000*end1.tv_sec+end1.tv_nsec/1000));
}

long long RTotalTime=0;
void compute_readlatency(){   
    RTotalTime = RTotalTime + ((1000000*end4.tv_sec+end4.tv_nsec/1000) - (1000000*end3.tv_sec+end3.tv_nsec/1000));
}
*/

struct ibv_mr hash_mr[2];
struct ibv_mr pointer_mr[128];
struct addrinfo addrinfo_g;


void extractbucket(struct context * ctx, Bucket ht){
	memcpy(ht, ctx->recv_buffer+sizeof(struct Message),sizeof(struct Bucket_));
}

void extractmr(struct context * ctx, struct Message msg){
	memcpy((void*)(&(ctx->remote_mr)), (void *)(&(msg.mr)), sizeof(struct ibv_mr));
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}


int on_completion(struct ibv_wc *wc);
void post_receives(struct context* ctx);
void recv_msg(struct context *ctx);
int send_msg(struct context *ctx, struct Message* msg);
void * poll_cq(void *);
void * build_context(struct ibv_context *verbs);
void build_qp_attr(struct context *ctx, struct ibv_qp_init_attr *qp_attr);
void register_memory(struct context *ctx);
int on_addr_resolved(struct rdma_cm_id *id);
int on_route_resolved(struct rdma_cm_id *id);
int on_connection(struct rdma_cm_id *id);
int on_disconnect(struct rdma_cm_id *id);
int on_event(struct rdma_cm_event *event);
void* main_loop(void *ctx_v);
int send_metadata(struct context *ctx);
void write_next_buffer(struct context *ctx);
int write_hash_remote(struct context *ctx, uint1_t hnumber, size_t id);
int write_remote(struct context *ctx, int offset);
int read_hash_remote(struct context *ctx, int len, uint64_t offset);
int read_remote(struct context *ctx, int len, uint64_t offset, int hid);


int main(int argc, char **argv){
	struct addrinfo *addr;
	struct rdma_cm_event *event = NULL;
	struct rdma_cm_id *conn = NULL;
	struct rdma_event_channel *ec = NULL;

	TEST_NZ(getaddrinfo(argv[1],argv[2],NULL,&addr));
	memcpy(&addrinfo_g,addr,sizeof(struct addrinfo));
	TEST_Z(ec = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_resolve_addr(conn,NULL,addr->ai_addr, TIMEOUT_IN_MS));
	printf("start\n");
	freeaddrinfo(addr);
	while(rdma_get_cm_event(ec,&event)==0){
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event,sizeof(*event));
		rdma_ack_cm_event(event);

		if(on_event(&event_copy)){
			break;
		}
	}
	rdma_destroy_event_channel(ec);
	return 0;
}

void* main_loop(void *ctx_v)
{
	void * ctx_t;
	struct context *ctx = (struct context *)ctx_v;
	struct ibv_wc wc;
 	//printf("ctx->cq before:%d\n",ctx->cq);
 	//printf("main loop\n");
 	ctx->tr.state = ESTABLISHED;
 	while(1){
	 	TEST_NZ(ibv_get_cq_event(ctx->comp_channel, &ctx->cq, &ctx_t));
	 	//printf("ctx->cq after:%d\n",ctx->cq);
	 	ibv_ack_cq_events(ctx->cq,1);
	 	TEST_NZ(ibv_req_notify_cq(ctx->cq,0));
	 	int num;
	 	while(num = ibv_poll_cq(ctx->cq,1,&wc)){
	 		//printf("new cq:%d\n",num);
	 		int ret = on_completion(&wc);
	 		//printf("wait for cq\n");
	 		if(ret){
	 			break;
	 		}
	 	}
 	}
 	printf("print!!! break\n");
 	return 0;
}


int write_hash_remote(struct context *ctx, uint1_t hnumber, size_t id)
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset( &wr,0,sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.send_flags = IBV_SEND_SIGNALED;

    wr.wr.rdma.remote_addr = (uintptr_t)hash_mr[hnumber].addr+0x18+id*0x20;
	wr.wr.rdma.rkey = hash_mr[hnumber].rkey;

	//printf("MR:%x,%x\n",wr.wr.rdma.rkey,wr.wr.rdma.remote_addr);
	if(ctx->hbuffer){
		wr.sg_list = &sge;
		wr.num_sge = 1;
		sge.addr = (uintptr_t)ctx->hbuffer;
		sge.length = 8;
		sge.lkey = ctx->hbuffer_mr->lkey;
	}
 	TEST_NZ(ibv_post_send(ctx->qp, &wr, &bad_wr));
}

int write_remote(struct context *ctx, int offset)
{
	struct ibv_send_wr wr, *bad_wr = NULL;

	struct ibv_sge sge;

	//printf("%s\n",ctx->buffer);
	memset( &wr,0,sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.send_flags = IBV_SEND_SIGNALED;

	wr.wr.rdma.remote_addr = (uintptr_t)ctx->remote_mr.addr+offset;
	wr.wr.rdma.rkey = ctx->remote_mr.rkey;
	//printf("MR:%x,%x\n",ctx->remote_mr.rkey,ctx->remote_mr.addr);
	if(strlen(ctx->buffer)){
		wr.sg_list = &sge;
		wr.num_sge = 1;
		sge.addr = (uintptr_t)ctx->buffer;
		sge.length = strlen(ctx->buffer)+1;
		sge.lkey = ctx->buffer_mr->lkey;
	}
 	TEST_NZ(ibv_post_send(ctx->qp, &wr, &bad_wr));
}

int read_hash_remote(struct context *ctx, int len, uint64_t offset)
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	memset(ctx->fbuffer, 0, len);

	memset(&wr,0,sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.opcode = IBV_WR_RDMA_READ;
	wr.send_flags = IBV_SEND_SIGNALED;

	wr.wr.rdma.remote_addr = (uintptr_t)hash_mr[0].addr+offset;
	wr.wr.rdma.rkey = hash_mr[0].rkey;
	//printf("read remote mr:%x,%x\n",wr.wr.rdma.rkey,wr.wr.rdma.remote_addr);
	if(len){
		wr.sg_list = &sge;
		wr.num_sge = 1;
		sge.addr = (uintptr_t)ctx->fbuffer;
		sge.length = len;
		sge.lkey = ctx->fbuffer_mr->lkey;
	}
 	TEST_NZ(ibv_post_send(ctx->qp, &wr, &bad_wr));
}

int read_remote(struct context *ctx, int len, uint64_t offset, int hid)
{
	//printf("%d:%d:%s\n",len, offset, chunk);
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	memset(ctx->buffer, 0, len);

	memset(&wr,0,sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.opcode = IBV_WR_RDMA_READ;
	wr.send_flags = IBV_SEND_SIGNALED;

	wr.wr.rdma.remote_addr = (uintptr_t)pointer_mr[hid].addr+offset;
	wr.wr.rdma.rkey = pointer_mr[hid].rkey;
	//printf("read remote mr:%x,%x\n",wr.wr.rdma.rkey,wr.wr.rdma.remote_addr);
	if(len){
		wr.sg_list = &sge;
		wr.num_sge = 1;
		sge.addr = (uintptr_t)ctx->buffer;
		sge.length = len;
		sge.lkey = ctx->buffer_mr->lkey;
	}
 	TEST_NZ(ibv_post_send(ctx->qp, &wr, &bad_wr));
}

void write_with_imm(struct context *ctx,int offset){
	//printf("write with imm\n");
 	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	memset( &wr,0,sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = htonl(0);

	wr.wr.rdma.remote_addr = (uintptr_t)ctx->remote_mr.addr;
	wr.wr.rdma.rkey = ctx->remote_mr.rkey;	
	TEST_NZ(ibv_post_send(ctx->qp, &wr, &bad_wr));

}


int count[128] = {0};
int on_completion(struct ibv_wc *wc)
{
 	struct context *ctx = (struct context *)(uintptr_t)wc->wr_id;
  	//printf("wc->status:%d\n",wc->status);
  	if (wc->status != IBV_WC_SUCCESS){
  		printf("1!!!\n");
  		return 0;
    }

  	  if (wc->opcode & IBV_WC_RECV){ 
  		struct Message msg_tmp ;
  		//printf("recv MSG: ");
  		memcpy(&msg_tmp, ctx->recv_buffer,sizeof(struct Message));
  		if(msg_tmp.type == WRITEBUFFEROK){
  			//wcount++;
  			//printf("Write buffer success\n");
			return 0;
  		}
  		if(msg_tmp.type == READOK){
  			//rcount++;
  			//printf("Read success\n");
			return 0;
  		}
  		if(msg_tmp.type == REGRBUFOK){
  			printf("REGRBUFOK\n");
			return 0;
  		}
   		
   		if(msg_tmp.type == INSETW){
   			//printf("INSETW: no hash bucket\n");			
			return 0;
  		}
  		if(msg_tmp.type == H1ADDR){
  			printf("H1ADDR\n");
  			memcpy((void*)(&(hash_mr[0])), (void *)(&(msg_tmp.mr)), sizeof(struct ibv_mr));
			//printf("remote hash1 mr:%x:%x\n",hash_mr[0].rkey,hash_mr[0].addr);
			return 0;
  		}
  		if(msg_tmp.type == H2ADDR){
  			printf("H2ADDR\n");
  			memcpy((void*)(&(hash_mr[1])), (void *)(&(msg_tmp.mr)), sizeof(struct ibv_mr));
			//extractmr(ctx, msg_tmp);
			//printf("remote hash2 mr:%x:%x\n",hash_mr[1].rkey,hash_mr[1].addr);
			return 0;
  		}
	  }

	  else if (wc->opcode == IBV_WC_SEND){
		if(ctx->tr.state == ESTABLISHED){

			ctx->tr.state = SENDMETADATA;
		}
		else if(ctx->tr.state == RECVMR){

			ctx->tr.state = RECVMR;	
		}
		else if(ctx->tr.state == FINISH){
			//gettime();
			on_disconnect(ctx->id);
		}
	  }
    
	  
	else if(wc->opcode == IBV_WC_RDMA_WRITE){
		//printf("WRITE DATA: hash-%p or data-%s write success\n",*(ctx->hbuffer), ctx->buffer);
		return 0;
        //struct Message msg;
		//msg.type = WS;
		//send_msg(ctx,&msg);
	}

	else if(wc->opcode == IBV_WC_RDMA_READ){
		//printf("READ DATA: hash-%s %d %p or data-%s read success\n", ctx->fbuffer->key,ctx->fbuffer->hid,ctx->fbuffer->mark,ctx->buffer);
        return 0;
	}

	else
      die("on_completion: completion isn't a send or a receive.");
   
    return 0;
}



int send_msg(struct context * ctx, struct Message *msg)
{
	struct ibv_send_wr wr,*bad_wr = NULL;
	struct ibv_sge sge;
	memset(ctx->send_buffer,0,sizeof(struct Message));
	memcpy(ctx->send_buffer,msg, sizeof(struct Message));
	//ctx->send_buffer[sizeof(struct Message)] = '0';
	//printf("send %s",msg);
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;
	sge.lkey = ctx->send_mr->lkey;
	sge.addr = (uintptr_t)ctx->send_buffer;
	sge.length = MSG_SIZE;

	int rc;

	TEST_NZ(rc = ibv_post_send(ctx->qp, &wr, &bad_wr));
	post_receives(ctx);

	return rc;
}


void * poll_cq(void *arg1)
{
	//useless in this situation
	
	struct ibv_wc wc;
	struct context *ctx = (struct context *)arg1;
	
	void *ctx_t=NULL;
	int i;
	 while(1){
	 	TEST_NZ(ibv_get_cq_event(ctx->comp_channel, &ctx->cq, &ctx_t));
	 	ibv_ack_cq_events(ctx->cq,1);
	 	TEST_NZ(ibv_req_notify_cq(ctx->cq,0));
	 	while(i = ibv_poll_cq(ctx->cq,1,&wc)){
	 		on_completion(&wc);
	 		//printf("on completion\n");
	 	}
	 }
	 return NULL;
}
 
void post_receives(struct context* ctx)
{
	struct ibv_recv_wr wr, *bad_wr = NULL;
	//printf("post receives\n"); 
	struct ibv_sge sge;
  	memset(&wr,0,sizeof(wr));
	wr.wr_id = (uintptr_t)ctx;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	sge.addr = (uintptr_t)ctx->recv_buffer;
	sge.length = MSG_SIZE;
	sge.lkey = ctx->recv_mr->lkey;
	TEST_NZ(ibv_post_recv(ctx->qp,&wr,&bad_wr));
}

void * build_context(struct ibv_context *verbs)
{
	struct context *ctx = (struct context *)malloc(sizeof(struct context));
	ctx->ctx = verbs;
	TEST_Z(ctx->pd = ibv_alloc_pd(ctx->ctx));
	TEST_Z(ctx->comp_channel = ibv_create_comp_channel(ctx->ctx));
	TEST_Z(ctx->cq = ibv_create_cq(ctx->ctx,16000,NULL,ctx->comp_channel,0));
	TEST_NZ(ibv_req_notify_cq(ctx->cq,0));

	TEST_NZ(pthread_create(&ctx->poll_thread,NULL,main_loop,ctx));

	return ctx;
}

void build_qp_attr(struct context *ctx, struct ibv_qp_init_attr *qp_attr)
{
	memset(qp_attr, 0, sizeof(*qp_attr));
	qp_attr->send_cq = ctx->cq;
	qp_attr->recv_cq = ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;
	qp_attr->cap.max_send_wr = 16000;
	qp_attr->cap.max_recv_wr = 16000;
	qp_attr->cap.max_send_sge = 1;
	qp_attr->cap.max_recv_sge = 1;
}

void register_memory(struct context *ctx)
{
	//printf("register_memory\n");
	ctx->send_buffer = malloc(MSG_SIZE);
	ctx->recv_buffer = malloc(MSG_SIZE);
	ctx->buffer = malloc(BUFFER_SIZE);
    ctx->hbuffer = malloc(8);
    ctx->fbuffer = malloc(32);
	TEST_Z(ctx->send_mr = ibv_reg_mr(
		ctx->pd,
		ctx->send_buffer,
		MSG_SIZE,
		IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE));
	TEST_Z(ctx->recv_mr = ibv_reg_mr(
		ctx->pd,
		ctx->recv_buffer,
		MSG_SIZE,
		IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE));
	TEST_Z(ctx->buffer_mr = ibv_reg_mr(
	 	ctx->pd,
	 	ctx->buffer,
	 	BUFFER_SIZE,
	 	IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));
    TEST_Z(ctx->hbuffer_mr = ibv_reg_mr(
	 	ctx->pd,
	 	ctx->hbuffer,
	 	8,
	 	IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));
   TEST_Z(ctx->fbuffer_mr = ibv_reg_mr(
	 	ctx->pd,
	 	ctx->fbuffer,
	 	32,
	 	IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ));
}

int on_route_resolved(struct rdma_cm_id *id)
{
	struct rdma_conn_param cm_params;
	//printf("route resolved.\n");
	memset(&cm_params,0,sizeof(cm_params));
	//why this is written for RDMA Write?
	cm_params.initiator_depth = cm_params.responder_resources =1;
	cm_params.rnr_retry_count = 7;
	//specific params here.
	TEST_NZ(rdma_connect(id,&cm_params));
	return 0;
}

int on_addr_resolved(struct rdma_cm_id *id)
{
	struct ibv_qp_init_attr qp_attr;
	struct context * ctx;
	
	//printf("addr resolved.\n");
	id->context = build_context(id->verbs);
	ctx = (struct context *)id->context;

	build_qp_attr(ctx,&qp_attr);
	TEST_NZ(rdma_create_qp(id,ctx->pd , &qp_attr));
	ctx->id = id;
	ctx->qp = id->qp;
 	ctx->num_completions = 0;

	register_memory(ctx);
	//post_receives(ctx);
	TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));
	return 0;
}



void reg_rbuf(struct context * ctx){
	struct Message* msg = (struct Message *)malloc(sizeof(struct Message));
	msg->type = REGRBUF;
	send_msg(ctx, msg);	 
}

void put(struct context * ctx, const char * key, const char * value){
    //printf("PUT:%s\n",key);
	struct Message* msg = (struct Message *)malloc(sizeof(struct Message));
	msg->type = PUT;
    sprintf(msg->data,"%s%s",key,value);	
	msg->datasize = strlen(msg->data)+1;
	
	send_msg(ctx, msg);	
}

void get(struct context * ctx, const char * key){
	//printf("GET:%s\n",key);
	struct Message* msg = (struct Message *)malloc(sizeof(struct Message));
	msg->type = GET;
	strcpy(msg->data,key);
	msg->datasize = strlen(msg->data)+1;
	send_msg(ctx, msg);	
}

int i;
void load_workloads(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta_load.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
            usleep(500);  
            strncpy(value, buf+27, valuesize);
            value[valuesize] = '\0'; 
            put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);            
       }
       
    }
    fclose(workload_1);
}

void run_workloads(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
            usleep(500);  
            strncpy(value, buf+27, valuesize);
            value[valuesize] = '\0'; 
            put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);            
       }
       
    }
    fclose(workload_1);
}

void run_workloads0(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta0_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
            usleep(500);  
            strncpy(value, buf+27, valuesize);
            value[valuesize] = '\0'; 
            put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);            
       }
       
    }
    fclose(workload_1);
}

void run_workloads1(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta1_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
            usleep(500);  
            strncpy(value, buf+27, valuesize);
            value[valuesize] = '\0'; 
            put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);            
       }
       
    }
    fclose(workload_1);
}

void run_workloads2(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta2_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads3(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta3_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads4(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta4_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads5(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta5_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads6(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta6_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads7(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta7_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads8(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta8_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads9(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta9_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads10(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta10_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads11(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta11_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads12(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta12_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads13(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta13_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}

void run_workloads14(struct context *ctx){
    FILE* workload_1;
    workload_1 = fopen("dataseta14_run.txt", "r");
	
	if(workload_1 == NULL){
        printf("couldn't open file");
        exit(1);
    }

    int BUF_LENGTH = valuesize+28;
    char buf[BUF_LENGTH];
    char *opcode = malloc(7);
    char *key = malloc(20);
    char *value = malloc(valuesize+1);
    while(fgets(buf,BUF_LENGTH,workload_1)){
        //for(i=0;i<10000;i++);
        strncpy(opcode, buf, 6);
        opcode[6] = '\0';
        strncpy(key, buf+7, 19);
        key[19] = '\0';
        if(strcmp(opcode,"INSERT") == 0 || strcmp(opcode,"UPDATE") == 0){
        	//for(i=0;i<100000;i++);    
                usleep(500);  
                strncpy(value, buf+27, valuesize);
                value[valuesize] = '\0'; 
                put(ctx,key,value);
                
        }
        else if(strcmp(opcode,"READAA") == 0){
            //for(i=0;i<100000;i++);   
            usleep(100);   
            get(ctx,key);
            
       }
       
    }
    fclose(workload_1);
}



int on_connection(struct rdma_cm_id *id)
{
	struct context *ctx = (struct context *)id->context;
	printf("connection established.\n");
	reg_rbuf(ctx);
	usleep(3000);
	
	pthread_t t[16];
   	
    load_workloads(ctx);
    printf("--------------------------------------\n");
    sleep(3);
    
    settime();
    pthread_create(&t[0],NULL,(void *)run_workloads,ctx);
    pthread_create(&t[1],NULL,(void *)run_workloads0,ctx);
    /*pthread_create(&t[2],NULL,(void *)run_workloads1,ctx);
    pthread_create(&t[3],NULL,(void *)run_workloads2,ctx);
    pthread_create(&t[4],NULL,(void *)run_workloads3,ctx);
    pthread_create(&t[5],NULL,(void *)run_workloads4,ctx);
    pthread_create(&t[6],NULL,(void *)run_workloads5,ctx);
    pthread_create(&t[7],NULL,(void *)run_workloads6,ctx);
    pthread_create(&t[8],NULL,(void *)run_workloads7,ctx);
    pthread_create(&t[9],NULL,(void *)run_workloads8,ctx);
    pthread_create(&t[10],NULL,(void *)run_workloads9,ctx);
    pthread_create(&t[11],NULL,(void *)run_workloads10,ctx);
    pthread_create(&t[12],NULL,(void *)run_workloads11,ctx);
    pthread_create(&t[13],NULL,(void *)run_workloads12,ctx);
    pthread_create(&t[14],NULL,(void *)run_workloads13,ctx);
    pthread_create(&t[15],NULL,(void *)run_workloads14,ctx);*/

    pthread_join(t[0],NULL);
    pthread_join(t[1],NULL);
    /*pthread_join(t[2],NULL);
    pthread_join(t[3],NULL);
    pthread_join(t[4],NULL);
    pthread_join(t[5],NULL);
    pthread_join(t[6],NULL);
    pthread_join(t[7],NULL);
    pthread_join(t[8],NULL);
    pthread_join(t[9],NULL);
    pthread_join(t[10],NULL);
    pthread_join(t[11],NULL);
    pthread_join(t[12],NULL);
    pthread_join(t[13],NULL);
    pthread_join(t[14],NULL);
    pthread_join(t[15],NULL);*/
    gettime();
	sleep(1);
	long long c = ((1000000*end.tv_sec+end.tv_nsec/1000) - (1000000*start.tv_sec+start.tv_nsec/1000));
	printf("%lld,%d\n",c,20000000000/c);
                
    //printf("%d,%d\n",wcount,rcount);
	


	//on_disconnect(id);

	return 0;
}


int on_disconnect(struct rdma_cm_id *id)
{
	struct context *ctx = (struct context *)id->context;
	printf("disconnected.\n");

	rdma_destroy_qp(id);
	ibv_dereg_mr(ctx->send_mr);
	ibv_dereg_mr(ctx->recv_mr);
	//ibv_dereg_mr(ctx->remote_mr);
	free(ctx->send_buffer);
	free(ctx->recv_buffer);
	free(ctx->buffer);
	free(ctx->hbuffer);
	free(ctx->fbuffer);
	free(ctx);
	rdma_destroy_id(id);
	return 1;
}

int on_event(struct rdma_cm_event *event)
{
	int r = 0;
	//printf("event\n");
	if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
		r = on_addr_resolved(event->id);
	else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
		r = on_route_resolved(event->id);
	else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
		r = on_connection(event->id);
	//	debug for 2h, just because pass the wrong arg event->id->context.
	else if (event->event == RDMA_CM_EVENT_DISCONNECTED){
		printf("1,event disconnect\n");
                r = on_disconnect(event->id);
        }
	else
		die("on_event: unknown event.");

	return r;
}




