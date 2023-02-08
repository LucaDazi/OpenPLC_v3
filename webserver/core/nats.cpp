#include <string>
#include <nats/nats.h>
#include <string.h>
#include <inttypes.h>

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
struct map*         variables;
struct map*         managed_outputs;

// char*               variable_names[BUFFER_SIZE];
int                 num_outputs = 0;

char (*last_output_value)[BUFFER_SIZE];

enum location {Input, Output, Memory, Undefined_Location};
enum size {Bit, Byte, Word, DWord, Long, Undefined_Size};

#define OPLC_CYCLE              25000000

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
    mapClose(variables);
    variables = mapNew();
    mapClose(managed_outputs);
    managed_outputs = mapNew();
    
}

void remove_address_tag(char *str) {
  int len = strlen(str);
  int value_len = strlen(".address");
  if (len >= value_len && strcmp(str + len - value_len, ".address") == 0) {
    str[len - value_len] = '\0';
  }
}

void remove_value_tag(char *str) {
  int len = strlen(str);
  int value_len = strlen(".value");
  if (len >= value_len && strcmp(str + len - value_len, ".value") == 0) {
    str[len - value_len] = '\0';
  }
}

static void*
getInternalOutputValue(const char* address)
{
    int scan = 0;
    location* var_loc = (location*)malloc(sizeof(enum location));
    size* var_size = (size*)malloc(sizeof(enum size));
    int main_add = 0;
    int bit_add = 0;

    *var_loc=Undefined_Location;
    *var_size=Undefined_Size;
    
    while (address[scan]!= '%' && address[scan] != '\0') scan++;
    scan++;
    switch(address[scan])
    {
        case 'I':
            *var_loc = Input;
            break;
        case 'Q':
            *var_loc = Output;
            break;
        case 'M':
            *var_loc = Memory;
            break;
        default:
            *var_loc = Undefined_Location;
    }

    scan++;
    
    switch(address[scan])
    {
        case 'X':
            *var_size = Bit;
        break;
        case 'B':
            *var_size = Byte;
        break;
        case 'W':
            *var_size = Word;
        break;
        case 'D':
            *var_size = DWord;
        break;
        case 'L':
            *var_size = Long;
        break;
        default:
            *var_size = Undefined_Size;
    }

    scan++;
    
    
    while(address[scan] != '.' && address[scan] != '\0')
    {
        main_add = main_add * 10 + (address[scan] - '0');
        scan++;
    }
    
    if(*var_size == Bit)
    {
        scan++;
        bit_add = address[scan] - '0';
    }
    
    void* result;
    switch(*var_size)
    {
        case Bit:
            if(*var_loc == Output)  
                result = bool_output[main_add][bit_add];
            else result = NULL;
        break;
        case Byte:
            if(*var_loc == Output) 
                result = byte_output[main_add];
            else result = NULL;
        break;
        case Word:
            if(*var_loc == Output) 
                result = int_output[main_add];
            else if(*var_loc == Memory) 
                result = int_memory[main_add];
            else
                result = NULL;
        break;
        case DWord:
            if(*var_loc == Memory) 
                result = dint_memory[main_add];
            else result = NULL;
        break;
        case Long:
            if(*var_loc == Memory) 
                result = lint_memory[main_add];
            else result = NULL;
        break;
        default: result = NULL;
    }
    
    free(var_loc);
    free(var_size);

    return result;
}

static void
updateInternalPLCValue(const char* address, const char* value)
{
    /*
    //Booleans
    extern IEC_BOOL *bool_input[BUFFER_SIZE][8];
    extern IEC_BOOL *bool_output[BUFFER_SIZE][8];

    //Bytes
    extern IEC_BYTE *byte_input[BUFFER_SIZE];
    extern IEC_BYTE *byte_output[BUFFER_SIZE];

    //Analog I/O
    extern IEC_UINT *int_input[BUFFER_SIZE];
    extern IEC_UINT *int_output[BUFFER_SIZE];

    //Memory
    extern IEC_UINT *int_memory[BUFFER_SIZE];
    extern IEC_DINT *dint_memory[BUFFER_SIZE];
    extern IEC_LINT *lint_memory[BUFFER_SIZE];    
    */

    int scan = 0;
    location* var_loc = (location*)malloc(sizeof(enum location));
    size* var_size = (size*)malloc(sizeof(enum size));
    int main_add = 0;
    int bit_add = 0;

    *var_loc=Undefined_Location;
    *var_size=Undefined_Size;
    
    while (address[scan]!= '%' && address[scan] != '\0') scan++;
    scan++;
    switch(address[scan])
    {
        case 'I':
            *var_loc = Input;
            break;
        case 'Q':
            *var_loc = Output;
            break;
        case 'M':
            *var_loc = Memory;
            break;
        default:
            *var_loc = Undefined_Location;
    }

    scan++;
    
    switch(address[scan])
    {
        case 'X':
            *var_size = Bit;
        break;
        case 'B':
            *var_size = Byte;
        break;
        case 'W':
            *var_size = Word;
        break;
        case 'D':
            *var_size = DWord;
        break;
        case 'L':
            *var_size = Long;
        break;
        default:
            *var_size = Undefined_Size;
    }

    scan++;
    
    
    while(address[scan] != '.' && address[scan] != '\0')
    {
        main_add = main_add * 10 + (address[scan] - '0');
        scan++;
    }
    
    if(*var_size == Bit)
    {
        scan++;
        bit_add = address[scan] - '0';
    }
    
    switch(*var_size)
    {
        case Bit:
            if(*var_loc == Input) 
                if (bool_input[main_add][bit_add] != NULL) *bool_input[main_add][bit_add] =(IEC_BOOL)(strcmp(value, "0") || strcmp(value, "true"));
            else if(*var_loc == Output) 
                if (bool_output[main_add][bit_add] != NULL) *bool_output[main_add][bit_add]=(IEC_BOOL)(strcmp(value, "0") || strcmp(value, "true"));
            else
            {
                sprintf((char*)log_msg, "In-memory boolean values are not supported! (%s)\n", address);
                log(log_msg);
            }
        break;
        case Byte:
            if(*var_loc == Input) 
                if (byte_input[main_add] != NULL) *byte_input[main_add] =(IEC_BYTE)atoi(value);
            else if(*var_loc == Output) 
                if (byte_output[main_add] != NULL) *byte_output[main_add]=(IEC_BYTE)atoi(value);
            else
            {
                sprintf((char*)log_msg, "In-memory byte values are not supported! (%s)\n", address);
                log(log_msg);
            }
        break;
        case Word:
            if(*var_loc == Input) 
                if (int_input[main_add] != NULL) *int_input[main_add] =(IEC_INT)atoi(value);
            else if(*var_loc == Output) 
                if (int_output[main_add] != NULL) *int_output[main_add]=(IEC_INT)atoi(value);
            else
                if (int_memory[main_add] != NULL) *int_memory[main_add]=(IEC_INT)atoi(value);
        break;
        case DWord:
            if(*var_loc == Memory) 
                if (dint_memory[main_add] != NULL) *dint_memory[main_add] =(IEC_DINT)strtol(value, NULL, 10);
            else
            {
                sprintf((char*)log_msg, "32 bit values are only supported in Memory locations! (%s)\n", address);
                log(log_msg);
            }
        break;
        case Long:
            if(*var_loc == Memory) 
            {
                if (lint_memory[main_add]!= NULL) *lint_memory[main_add] =(IEC_LINT)strtoll(value, NULL, 10);
            }
            else
            {
                sprintf((char*)log_msg, "64 bit values are only supported in Memory locations! (%s)\n", address);
                log(log_msg);
            }
        break;
    }
    
    free(var_loc);
    free(var_size);

}

static void
messageCallback(void *userdata, const char *name, size_t name_len,
                        const char *path, const struct json_token *t)
{
    //starts with ".data."
    if(strncmp(".data.", path, 6 ) == 0)
    {
        if(endsWith(path, ".value") != 0)        
        {
            //Variable found. Update internal structs.

            char* varName = strdup(path+6);
            
            remove_value_tag(varName);

            char* strValue = (char*)malloc(t->len * sizeof(char));
            sprintf((char*)strValue, "%.*s", t->len, t->ptr);

            char* addressing = (char*)mapGet(varName, variables);

            sprintf((char*)log_msg, "%s = %s\n", varName, addressing);
            log(log_msg);

            updateInternalPLCValue(addressing, strValue);
            
            free(varName);
        }
    }
}

static void
configFileWalkCb(void *userdata, const char *name, size_t name_len,
                        const char *path, const struct json_token *t)
{    
    //starts with ".data."
    if(strncmp(".data.", path, 6 ) == 0)
    {
        if(endsWith(path, ".address") != 0)        
        {
            //Variable found. Add to PLC memory mapping.
            char* varName = strdup(path+6);
            
            remove_address_tag(varName);

            
            char* mapping = (char*)malloc(t->len * sizeof(char));
            sprintf((char*)mapping, "%.*s", t->len, t->ptr);

            sprintf((char*)log_msg, "%s = %s\n", varName, mapping);
            log(log_msg);
            mapDynAdd(varName, mapping, variables);
            if(mapping[1]=='Q' || mapping[1]=='M')
            {
                mapDynAdd(varName, mapping, managed_outputs);            
                num_outputs++;
            }

            free(varName);
        }
    }
}

static void
loadMappingFile()
{
    char* jsonFile = json_fread("../config/config.json");

    int fileLen = strlen(jsonFile);
    sprintf((char*)log_msg, "Lenght is %d\n", fileLen);
    log(log_msg);

    
    
    sprintf((char*)log_msg, "JSON WALK\n");
    log(log_msg);
    emptyVariables();
    num_outputs = 0;
    
    json_walk(jsonFile, strlen(jsonFile), configFileWalkCb, NULL);
    last_output_value = (char(*)[BUFFER_SIZE]) malloc(num_outputs * sizeof *last_output_value);

    sprintf((char*)log_msg, "Number of outputs stored: %d\n", num_outputs);
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

    // Locking input buffer to avoid conflicts with PLC program
    pthread_mutex_lock(&bufferLock);
    json_walk(natsMsg_GetData(msg), strlen(natsMsg_GetData(msg)), messageCallback, NULL);
    pthread_mutex_unlock(&bufferLock);
    
    natsMsg_Destroy(msg);

}

static natsOptions* generateOptions(const char* server){
    natsStatus  s       = NATS_OK;

    if (natsOptions_Create(&opts) != NATS_OK)
        s = NATS_NO_MEMORY;

    natsOptions_SetURL(opts, server);
    natsOptions_SetClosedCB(opts, asyncClosedCb, (void*)&closed);
    natsOptions_SetErrorHandler(opts, asyncErrorCb, NULL);
    natsOptions_SetName(opts, connectionName);
    natsOptions_SetDisconnectedCB(opts, asyncDisconnectedCb, NULL);
    natsOptions_SetReconnectedCB(opts, asyncReconnectedCb, NULL);
    natsOptions_SetReconnectJitter(opts, 250, 5000);
    natsOptions_SetReconnectWait(opts, 500);
    natsOptions_SetRetryOnFailedConnect(opts, true, asyncReconnectedCb, NULL);
}

void natsStartClient(char* server, char* sub_topic, char* pub_topic){
    sprintf((char*)log_msg, "Starting NATS Client\n");
    log(log_msg);

    natsConnection      *conn = NULL;
    natsOptions         *opts  = NULL;
    natsSubscription    *sub   = NULL;
    natsStatistics      *stats = NULL;
    natsMsg             *msg   = NULL;
    natsStatus          s;

    variables = mapNew();
    managed_outputs = mapNew();
    
    loadMappingFile();

    opts = generateOptions(server);
    s = natsConnection_Connect(&conn, opts);

    sprintf((char*)log_msg, "Listening asynchronously on '%s'.\n", sub_topic);
    log(log_msg);

    if (s == NATS_OK)
    {
        s = natsConnection_Subscribe(&sub, conn, sub_topic, onMsg, NULL);
    }

    // For maximum performance, set no limit on the number of pending messages.
    if (s == NATS_OK)
        s = natsSubscription_SetPendingLimits(sub, -1, -1);

    // Continuously update
    struct timespec timer_start;
    clock_gettime(CLOCK_MONOTONIC, &timer_start);
    
    dataToSend = mapNew();
    bool firstRun = true;
    while(run_nats) 
    {
        pthread_mutex_lock(&bufferLock);
        //CHECK VARIABLES AND FIRE COMMANDS TO NATS IF NEEDED
        mapClose(dataToSend);
        dataToSend = mapNew();
        int dataToSendLen = 0;

        for(int i=0; i<mapSize(managed_outputs); i++){
            char* output = (char*)mapGet(i, managed_outputs);
            char* result = (char*)getInternalOutputValue(output);
            if(result == NULL) continue;
            
            if(strcmp(last_output_value[i], result) != 0)
            {
                mapAdd((char*)mapGetKey(i, managed_outputs), result, dataToSend);
                *last_output_value[i] = *result;
                dataToSendLen++;                
            }
            

        }
        pthread_mutex_unlock(&bufferLock);
        
        if(dataToSendLen > 0){
            if(firstRun)
                firstRun = false;
            else
            {
                strcpy(nats_payload, "{\"specversion\": \"1.0\",\"id\": \"fac68cf9-dead-beef-8d2a-4adb55868046\",\"type\": \"deviceservice.samples\",\"source\": \"openplc://devices/simulator\",\"datacontenttype\": \"application/json\",\"subject\": \"breaker\",\"data\": {");
                for(int i=0; i<mapSize(dataToSend); i++){                    
                    sprintf((char*)log_msg, "\"%s\":{\"value\":%s},");
                    strcat(nats_payload, (char*)log_msg);
                };
                nats_payload[strlen(nats_payload)-1]='\0';
                strcat(nats_payload, "}}");
                //Generate and publish command
                natsConnection_Publish(conn, pub_topic, nats_payload, strlen(nats_payload));
            }
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