//
// Created by Yufei on 2024/9/5.
//

#ifndef TEE_CONNECTION_H
#define TEE_CONNECTION_H

#include <err.h>
#include <string.h>
#include <iostream>

/* OP-TEE TEE client API (built by optee_client) */
#include <tee_client_api.h>

/* For the UUID (found in the TA's h-file(s)) */
#include "tee_connection_ta.h"


class TEE_connection {
  public:
    TEE_connection(int32_t id): id(id) {

     /* Initialize a context connecting us to the TEE */
     res = TEEC_InitializeContext(NULL, &ctx);
     if (res != TEEC_SUCCESS)
      errx(1, "TEEC_InitializeContext failed with code 0x%x", res);

     /*
      * Open a session to the "hello world" TA, the TA will print "hello
      * world!" in the log when the session is created.
      */
     res = TEEC_OpenSession(&ctx, &sess, &uuid,
                    TEEC_LOGIN_PUBLIC, NULL, NULL, &err_origin);
     if (res != TEEC_SUCCESS)
      errx(1, "TEEC_Opensession failed with code 0x%x origin 0x%x",
          res, err_origin);
    }
    ~TEE_connection() {
      /*
       * We're done with the TA, close the session and
       * destroy the context.
       *
       * The TA will print "Goodbye!" in the log when the
       * session is closed.
       */
    //  TEEC_CloseSession(&sess);
    //  TEEC_FinalizeContext(&ctx);
    }

    void closeConnection(){
      TEEC_CloseSession(&sess);
      TEEC_FinalizeContext(&ctx);
    }

    bool is_equal(int first, int second){
      /*
       * Execute a function in the TA by invoking it, in this case
       * we're incrementing a number.
       *
       * The value of command ID part and how the parameters are
       * interpreted is part of the interface provided by the TA.
       */
      /* Clear the TEEC_Operation struct */
      memset(&op, 0, sizeof(op));
      /*
       * Prepare the argument. Pass a value in the first parameter,
       * the remaining three parameters are unused.
       */
      op.paramTypes = TEEC_PARAM_TYPES(TEEC_VALUE_INOUT, TEEC_VALUE_INOUT,
                                       TEEC_VALUE_INOUT, TEEC_NONE);
      op.params[0].value.a = first;
      op.params[1].value.a = second;
      op.params[2].value.a = 0;
      /*
       * TA_HELLO_WORLD_CMD_INC_VALUE is the actual function in the TA to be
       * called.
       */
      std::cout << "Invoking TA to compare" << op.params[0].value.a << " " << op.params[1].value.a << std::endl;
      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_COMPAIRE, &op,
                   &err_origin);
      if (res != TEEC_SUCCESS)
          errx(1, "TEEC_InvokeCommand failed with code 0x%x origin 0x%x",
              res, err_origin);
      std::cout << "TA results: " << op.params[2].value.a << std::endl;
      return op.params[2].value.a;
    }

    int decode(int ciphertext, int offset, int mod){
          /*
           * Execute a function in the TA by invoking it, in this case
           * we're incrementing a number.
           *
           * The value of command ID part and how the parameters are
           * interpreted is part of the interface provided by the TA.
           */

          /* Clear the TEEC_Operation struct */
          memset(&op, 0, sizeof(op));

          /*
           * Prepare the argument. Pass a value in the first parameter,
           * the remaining three parameters are unused.
           */
          op.paramTypes = TEEC_PARAM_TYPES(TEEC_VALUE_INOUT, TEEC_VALUE_INOUT,
                                           TEEC_NONE, TEEC_NONE);
          op.params[0].value.a = ciphertext;
          op.params[1].value.a = offset;
          op.params[1].value.b = mod;
          /*
           * TA_HELLO_WORLD_CMD_INC_VALUE is the actual function in the TA to be
           * called.
           */
          std::cout << "Invoking TA to decrypt " << op.params[0].value.a << ", offset " << op.params[1].value.a << ", mod " << op.params[1].value.b << std::endl;
          res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_DECODE_ID, &op,
                       &err_origin);
          if (res != TEEC_SUCCESS)
              errx(1, "TEEC_InvokeCommand failed with code 0x%x origin 0x%x",
                  res, err_origin);
          std::cout << "TA results: " << op.params[0].value.a << std::endl;
          return op.params[0].value.a;
      }

    void decrease(){
      memset(&op, 0, sizeof(op));
      op.paramTypes = TEEC_PARAM_TYPES(
              TEEC_VALUE_INOUT, TEEC_NONE, TEEC_NONE, TEEC_NONE);
      op.params[0].value.a = 12;
      op.params[0].value.b = 13;
      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_DEC_VALUE, &op, &err_origin);
      std::cout << op.params[0].value.a << std::endl;
      std::cout << op.params[0].value.b << std::endl;

    }

    double min(double a, double b){
      double * shared_data = (double *) shm.buffer;
      shared_data[0] = a;
      shared_data[1] = b;

      memset(&op, 0, sizeof(op));
      op.paramTypes = TEEC_PARAM_TYPES(TEEC_MEMREF_WHOLE, TEEC_NONE, TEEC_NONE, TEEC_NONE);
      op.params[0].memref.parent = &shm;

      res = TEEC_InvokeCommand(&sess, TA_TEE_CONNECTION_SHARED_MEM, &op, &err_origin);
      if (res != TEEC_SUCCESS) {
          printf("TEEC_InvokeCommand failed: 0x%x, origin: 0x%x\n", res, err_origin);
      } else {
          printf("Response from TA: %f\n", shared_data[0]);
      }
      return shared_data[0];

    }

    TEEC_Result allocate_shared_menary(size_t size, uint32_t flags = TEEC_MEM_INPUT | TEEC_MEM_OUTPUT){
      shm.size = size;
      shm.flags = flags;
      res = TEEC_AllocateSharedMemory(&ctx, &shm);
      if (res != TEEC_SUCCESS) {
        printf("TEEC_AllocateSharedMemory failed: 0x%x\n", res);
      }
      return res;
    }
  private:
    TEEC_Result res;
    TEEC_Context ctx;
    TEEC_Session sess;
    TEEC_Operation op;
    TEEC_UUID uuid = TA_TEE_CONNECTION_UUID;
    uint32_t err_origin;

    TEEC_SharedMemory shm;


    int32_t id;
};



#endif //TEE_CONNECTION_H
