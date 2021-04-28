#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "txn_table.h"

SundialRPCClient::SundialRPCClient() {
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    _servers = new SundialRPCClientStub * [g_num_nodes_and_storage];
#else
    _servers = new SundialRPCClientStub * [g_num_nodes];
#endif
    // get server names
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t node_id = 0;
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    while ( node_id  < g_num_nodes_and_storage && getline(in, line) )
#else
    while ( node_id  < g_num_nodes && getline(in, line) )
#endif
    {
        if (line[0] == '#')
            continue;
        else if (line[0] == '=' && line[1] == 'l')
            break;
        else {
            string url = line;
            if (node_id != g_node_id) {
                _servers[node_id] = new SundialRPCClientStub(grpc::CreateChannel(url,
                    grpc::InsecureChannelCredentials()));
                cout << "[Sundial] init rpc client - " << node_id << " at " << url << endl;
            }
            node_id ++;
        }
    }
    cout << "[Sundial] rpc client is initialized!" << endl;
    // spawn a reader thread to indefinitely read completion queue
    _thread = new std::thread(AsyncCompleteRpc, this);
    //pthread_create(_thread, NULL, AsyncCompleteRpc, this); 
    // use a single cq for different channels. 
}

void
SundialRPCClient::AsyncCompleteRpc(SundialRPCClient * s) {
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (s->cq.Next(&got_tag, &ok)) {
        // The tag in this example is the memory location of the call object
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
        if (!call->status.ok()) {
            printf("[REQ] client rec response fail: (%d) %s\n",
                   call->status.error_code(), call->status.error_message().c_str());
        }
        // handle return value for non-system response
        assert(call->reply->response_type() != SundialResponse::SYS_RESP);
        s->sendRequestDone(call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }
}

RC
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request,
    SundialResponse &response) {
    if (!glob_manager->active && (request.request_type() !=
    SundialRequest::SYS_REQ))
        return FAIL;
    ClientContext context;
    Status status = _servers[node_id]->contactRemote(&context, request, &response);
    if (!status.ok()) {
        printf("[REQ] client sendRequest fail: (%d) %s\n",
               status.error_code(), status.error_message().c_str());
        assert(false);
    }
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();
	return RCOK;
}

RC
SundialRPCClient::sendRequestAsync(TxnManager * txn, uint64_t node_id,
                                   SundialRequest &request, SundialResponse &response)
{
    if (!glob_manager->active && (request.request_type() !=
    SundialRequest::TERMINATE_REQ))
        return FAIL;
    assert(node_id != g_node_id);
    // call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;;
    assert(node_id != g_node_id);
    // RACE CONDITION: should assign thd id to server thread
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    call->response_reader = _servers[node_id]->stub_->PrepareAsynccontactRemote(&call->context, request, &cq);

    // StartCall initiates the RPC call
    call->response_reader->StartCall();
    call->reply = &response;
    call->response_reader->Finish(call->reply, &(call->status), (void*)call);
	return RCOK;
}


void
SundialRPCClient::sendRequestDone(SundialResponse * response)
{
    uint64_t txn_id = response->txn_id();
    TxnManager * txn = txn_table->get_txn(txn_id);
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response->response_type() ]++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response->response_type() ] += response->SpaceUsedLong();

    switch (response->request_type()) {
        case SundialResponse::READ_REQ :
            break;
        case SundialResponse::PREPARE_REQ : txn->handle_prepare_resp(response);
            break;
        case SundialResponse::TERMINATE_REQ:
            return;
        default:
            break;
    }

#if LOG_DEVICE == LOG_DEV_NATIVE
    if (IS_LOG_RESPONSE(response->response_type())) {
        // not decrease semaphore for log resp
        txn->rpc_log_semaphore->decr();
    } else
#endif
        txn->rpc_semaphore->decr();

}
