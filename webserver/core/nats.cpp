#include <string>
#include <nats/nats.h>
#include <string.h>

#include "ladder.h"
#include "map.h"

const char          *subSubj   = "devices.ethercat.tatv.events";
const char          *pubSubj   = "devices.ethercat.tatv.commands";
const char          *url = "nats://localhost:4222";
const char          *connectionName = "OpenPLC_client";
natsOptions         *opts   = NULL;
unsigned char log_msg[2047];
char nats_payload[262143];
volatile int64_t    dropped = 0;
bool                closed = false;
struct map*         dataToSend;
char*               variable_names[BUFFER_SIZE];
int                 num_variables = 0;

IEC_UINT last_int_output[BUFFER_SIZE];

#define OPLC_CYCLE              50000000

static void
asyncErrorCb(natsConnection *nc, natsSubscription *sub, natsStatus err, void *closure)
{
    sprintf((char*)log_msg, "Async error: %u - %s\n", err, natsStatus_GetText(err));
    log(log_msg);

    natsSubscription_GetDropped(sub, (int64_t*) &dropped);
}

static void
asyncClosedCb(natsConnection *nc, void *closure)
{
    bool        *closed = (bool*)closure;
    const char  *err    = NULL;

    natsConnection_GetLastError(nc, &err);
    sprintf((char*)log_msg, "Connect failed: %s\n", err);
    log(log_msg);

    *closed = true;
}

static void
asyncDisconnectedCb(natsConnection *nc, void *closure){
    sprintf((char*)log_msg, "%s disconnected from %s\n", connectionName, url);
    log(log_msg);
}

static void
asyncReconnectedCb(natsConnection *nc, void *closure){
    sprintf((char*)log_msg, "%s connected to %s\n", connectionName, url);    
    log(log_msg);
}

static int endsWith(const char *str, const char *suffix)
{
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix >  lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

void emptyVariables()
{
    for(int i=0; i<BUFFER_SIZE; i++){
        if(variable_names[i] != 0){
            free(variable_names[i]);
        }
    }
}

void remove_value(char *str) {
  int len = strlen(str);
  int value_len = strlen(".value");
  if (len >= value_len && strcmp(str + len - value_len, ".value") == 0) {
    str[len - value_len] = '\0';
  }
}

static void
asyncJsonWalkCb(void *userdata, const char *name, size_t name_len,
                        const char *path, const struct json_token *t){    
    //starts with ".data."
    if(strncmp(".data.", path, 6 ) == 0)
    {
        if(endsWith(path, ".value") != 0)        
        {
            //Variable found. Add to PLC memory mapping.
            char* varName = strdup(path);
            remove_value(varName);            
            sprintf((char*)log_msg, "%s = %.*s\n", varName, t->len, t->ptr);
            log(log_msg);
            unsigned char strValue[12];
            sprintf((char*)strValue, "%.*s", t->len, t->ptr);
            int value = atoi((char*)strValue);
            //INTERNAL PLC MEMORY
            int_memory[num_variables] = (IEC_UINT*)value;
            int_input[num_variables] = (IEC_UINT*)value;

            //VARIABLES MAP
            strcpy(variable_names[num_variables], varName);            
            num_variables++;

            free(varName);
        }
    }
}

static void
loadMappingFile()
{
    char* jsonFile = json_fread("payload_example.json");

    int fileLen = strlen(jsonFile);
    sprintf((char*)log_msg, "Lenght is %d\n", fileLen);
    log(log_msg);

    sprintf((char*)log_msg, "JSON WALK\n");
    log(log_msg);
    emptyVariables();
    num_variables = 0;
    json_walk(jsonFile, strlen(jsonFile), asyncJsonWalkCb, NULL);

    sprintf((char*)log_msg, "Number of variables stored: %d\n", num_variables);
    log(log_msg);

    free(jsonFile);
}

static void
onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    
    
    sprintf(nats_payload, "%s", natsMsg_GetData(msg));
    
    sprintf((char*)log_msg, "Received msg: %s - %.*s\n",
            natsMsg_GetSubject(msg),
            natsMsg_GetDataLength(msg),
            nats_payload);
    
    log(log_msg);

    loadMappingFile();
    /*
    sprintf((char*)log_msg, "Lenght is %d\n", strlen(natsMsg_GetData(msg)));
    log(log_msg);

    json_walk(natsMsg_GetData(msg), strlen(natsMsg_GetData(msg)), asyncJsonWalkCb, NULL);
    */
    natsMsg_Destroy(msg);

}

static natsOptions* generateOptions(){
    natsStatus  s       = NATS_OK;

    if (natsOptions_Create(&opts) != NATS_OK)
        s = NATS_NO_MEMORY;

    natsOptions_SetURL(opts, url);
    natsOptions_SetClosedCB(opts, asyncClosedCb, (void*)&closed);
    natsOptions_SetErrorHandler(opts, asyncErrorCb, NULL);
    natsOptions_SetName(opts, connectionName);
    natsOptions_SetDisconnectedCB(opts, asyncDisconnectedCb, NULL);
    natsOptions_SetReconnectedCB(opts, asyncReconnectedCb, NULL);
    natsOptions_SetReconnectJitter(opts, 250, 5000);
    natsOptions_SetReconnectWait(opts, 500);
    natsOptions_SetRetryOnFailedConnect(opts, true, asyncReconnectedCb, NULL);
}

void natsStartClient(int port){
    sprintf((char*)log_msg, "Starting NATS Client\n");
    log(log_msg);

    natsConnection      *conn = NULL;
    natsOptions         *opts  = NULL;
    natsSubscription    *sub   = NULL;
    natsStatistics      *stats = NULL;
    natsMsg             *msg   = NULL;
    natsStatus          s;

    s = natsConnection_Connect(&conn, opts);

    sprintf((char*)log_msg, "Listening asynchronously on '%s'.\n", subSubj);
    log(log_msg);

    if (s == NATS_OK)
    {
        s = natsConnection_Subscribe(&sub, conn, subSubj, onMsg, NULL);
    }

    // For maximum performance, set no limit on the number of pending messages.
    if (s == NATS_OK)
        s = natsSubscription_SetPendingLimits(sub, -1, -1);

    // Continuously update
    struct timespec timer_start;
    clock_gettime(CLOCK_MONOTONIC, &timer_start);
    
    dataToSend = mapNew();

    while(run_nats) 
    {
        pthread_mutex_lock(&bufferLock);
        //CHECK VARIABLES AND FIRE COMMANDS TO NATS IF NEEDED
        mapClose(dataToSend);
        dataToSend = mapNew();
        int dataToSendLen = 0;
        for(int i=0; i<BUFFER_SIZE; i++){
            if(int_output[i]){
                if(*int_output[i] != (IEC_UINT)last_int_output[i]){
                    //Output value changed by PLC program. Adding to list.
                    if(variable_names[i]){
                        mapAdd(variable_names[i], (int*)int_output[i], dataToSend);
                    }else{
                        mapAdd("UNNAMED", (int*)int_output[i], dataToSend);
                    }
                    dataToSendLen++;
                }
            }
        }              
        pthread_mutex_unlock(&bufferLock);
        if(dataToSendLen > 0){
            //Generate and publish command
            natsConnection_Publish(conn, pubSubj, "TEST COMMAND", 12);
        }
        sleep_until(&timer_start, OPLC_CYCLE);
    }

    sprintf((char*)log_msg, "Shutting down NATS client\n");
    log(log_msg);

    // Destroy all our objects to avoid report of memory leak
    natsSubscription_Unsubscribe(sub);    
    natsSubscription_Destroy(sub);

    natsConnection_Close(conn);
    natsConnection_Destroy(conn);
    natsOptions_Destroy(opts);

    // To silence reports of memory still in used with valgrind
    nats_Close();

}