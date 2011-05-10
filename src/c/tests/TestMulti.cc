/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cppunit/extensions/HelperMacros.h>
#include "CppAssertHelper.h"

#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/select.h>

#include "CollectionUtil.h"
#include "ThreadingUtil.h"

using namespace Util;

#include "Vector.h"
using namespace std;

#include <cstring>
#include <list>

#include <zookeeper.h>
#include <errno.h>
#include <recordio.h>
#include "Util.h"

struct buff_struct_2 {
    int32_t len;
    int32_t off;
    char *buffer;
};

#ifdef THREADED
    static void yield(zhandle_t *zh, int i)
    {
        sleep(i);
    }
#else
    static void yield(zhandle_t *zh, int seconds)
    {
        int fd;
        int interest;
        int events;
        struct timeval tv;
        int rc;
        time_t expires = time(0) + seconds;
        time_t timeLeft = seconds;
        fd_set rfds, wfds, efds;
        FD_ZERO(&rfds);
        FD_ZERO(&wfds);
        FD_ZERO(&efds);

        while(timeLeft >= 0) {
            zookeeper_interest(zh, &fd, &interest, &tv);
            if (fd != -1) {
                if (interest&ZOOKEEPER_READ) {
                    FD_SET(fd, &rfds);
                } else {
                    FD_CLR(fd, &rfds);
                }
                if (interest&ZOOKEEPER_WRITE) {
                    FD_SET(fd, &wfds);
                } else {
                    FD_CLR(fd, &wfds);
                }
            } else {
                fd = 0;
            }
            FD_SET(0, &rfds);
            if (tv.tv_sec > timeLeft) {
                tv.tv_sec = timeLeft;
            }
            rc = select(fd+1, &rfds, &wfds, &efds, &tv);
            timeLeft = expires - time(0);
            events = 0;
            if (FD_ISSET(fd, &rfds)) {
                events |= ZOOKEEPER_READ;
            }
            if (FD_ISSET(fd, &wfds)) {
                events |= ZOOKEEPER_WRITE;
            }
            zookeeper_process(zh, events);
        }
    }
#endif

typedef struct evt {
    string path;
    int type;
} evt_t;

typedef struct watchCtx {
private:
    list<evt_t> events;
    watchCtx(const watchCtx&);
    watchCtx& operator=(const watchCtx&);
public:
    bool connected;
    zhandle_t *zh;
    Mutex mutex;

    watchCtx() {
        connected = false;
        zh = 0;
    }
    ~watchCtx() {
        if (zh) {
            zookeeper_close(zh);
            zh = 0;
        }
    }

    evt_t getEvent() {
        evt_t evt;
        mutex.acquire();
        CPPUNIT_ASSERT( events.size() > 0);
        evt = events.front();
        events.pop_front();
        mutex.release();
        return evt;
    }

    int countEvents() {
        int count;
        mutex.acquire();
        count = events.size();
        mutex.release();
        return count;
    }

    void putEvent(evt_t evt) {
        mutex.acquire();
        events.push_back(evt);
        mutex.release();
    }

    bool waitForConnected(zhandle_t *zh) {
        time_t expires = time(0) + 10;
        while(!connected && time(0) < expires) {
            yield(zh, 1);
        }
        return connected;
    }
    bool waitForDisconnected(zhandle_t *zh) {
        time_t expires = time(0) + 15;
        while(connected && time(0) < expires) {
            yield(zh, 1);
        }
        return !connected;
    }
} watchctx_t;

class Zookeeper_multi : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(Zookeeper_multi);
//FIXME: None of these tests pass in single-threaded mode. It seems to be a
//flaw in the test suite setup.
#ifdef THREADED
    CPPUNIT_TEST(testCreate);
    CPPUNIT_TEST(testCreateDelete);
    CPPUNIT_TEST(testInvalidVersion);
    CPPUNIT_TEST(testNestedCreate);
    CPPUNIT_TEST(testSetData);
    CPPUNIT_TEST(testUpdateConflict);
    CPPUNIT_TEST(testDeleteUpdateConflict);
    CPPUNIT_TEST(testAsyncMulti);
    CPPUNIT_TEST(testMultiFail);
    CPPUNIT_TEST(testCheck);
#endif
    CPPUNIT_TEST_SUITE_END();

    static void watcher(zhandle_t *, int type, int state, const char *path,void*v){
        watchctx_t *ctx = (watchctx_t*)v;

        if (state == ZOO_CONNECTED_STATE) {
            ctx->connected = true;
        } else {
            ctx->connected = false;
        }
        if (type != ZOO_SESSION_EVENT) {
            evt_t evt;
            evt.path = path;
            evt.type = type;
            ctx->putEvent(evt);
        }
    }

    static const char hostPorts_multi[];

    const char *getHostPorts() {
        return hostPorts_multi;
    }
    
    zhandle_t *createClient(watchctx_t *ctx) {
        return createClient(hostPorts_multi, ctx);
    }

    zhandle_t *createClient(const char *hp, watchctx_t *ctx) {
        zhandle_t *zk = zookeeper_init(hp, watcher, 10000, 0, ctx, 0);
        ctx->zh = zk;
        sleep(1);
        return zk;
    }
    
    zhandle_t *createchClient(watchctx_t *ctx, const char* chroot) {
        zhandle_t *zk = zookeeper_init(chroot, watcher, 10000, 0, ctx, 0);
        ctx->zh = zk;
        sleep(1);
        return zk;
    }
        
    FILE *logfile;
public:

    Zookeeper_multi() {
      logfile = openlogfile("Zookeeper_multi");
    }

    ~Zookeeper_multi() {
      if (logfile) {
        fflush(logfile);
        fclose(logfile);
        logfile = 0;
      }
    }

    void setUp()
    {
        zoo_set_log_stream(logfile);
    }


    void startServer() {
        char cmd[1024];
        sprintf(cmd, "%s start %s", ZKSERVER_CMD, getHostPorts());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void stopServer() {
        char cmd[1024];
        sprintf(cmd, "%s stop %s", ZKSERVER_CMD, getHostPorts());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void tearDown()
    {
    }
    
    /** have a callback in the default watcher **/
    static void default_zoo_watcher(zhandle_t *zzh, int type, int state, const char *path, void *context){
        int zrc = 0;
        struct String_vector str_vec = {0, NULL};
        zrc = zoo_wget_children(zzh, "/mytest", default_zoo_watcher, NULL, &str_vec);
    }
  
    bool waitForEvent(zhandle_t *zh, watchctx_t *ctx, int seconds) {
        time_t expires = time(0) + seconds;
        while(ctx->countEvents() == 0 && time(0) < expires) {
            yield(zh, 1);
        }
        return ctx->countEvents() > 0;
    }

#define COUNT 100

    static volatile int count;
    static const char* hp_chroot;

    static void statCompletion(int rc, const struct Stat *stat, const void *data) {
        int tmp = (int) (long) data;
        CPPUNIT_ASSERT_EQUAL(tmp, rc);
    }

    static void create_completion_fn(int rc, const char* value, const void *data) {
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        count++;
    }

    static void multi_completion_fn(int rc, const void *data) {
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        count++;
    }

    static void waitForCreateCompletion(int seconds) {
        time_t expires = time(0) + seconds;
        while(count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void waitForMultiCompletion(int seconds) {
        time_t expires = time(0) + seconds;
        while(count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void watcher_chroot_fn(zhandle_t *zh, int type,
                                    int state, const char *path,void *watcherCtx) {
        // check for path
        char *client_path = (char *) watcherCtx;
        CPPUNIT_ASSERT(strcmp(client_path, path) == 0);
        count ++;
    }

    static void waitForChrootWatch(int seconds) {
        time_t expires = time(0) + seconds;
        while (count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void waitForVoidCompletion(int seconds) {
        time_t expires = time(0) + seconds;
        while(count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void voidCompletion(int rc, const void *data) {
        int tmp = (int) (long) data;
        CPPUNIT_ASSERT_EQUAL(tmp, rc);
        count++;
    }

    static void verifyCreateFails(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZBADARGUMENTS, zoo_create(zk,
          path, "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0));
    }

    static void verifyCreateOk(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZOK, zoo_create(zk,
          path, "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0));
    }

    static void verifyCreateFailsSeq(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZBADARGUMENTS, zoo_create(zk,
          path, "", 0, &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, 0, 0));
    }

    static void verifyCreateOkSeq(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZOK, zoo_create(zk,
          path, "", 0, &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, 0, 0));
    }

    /**
     * Test basic multi-op create functionality 
     */
    void testCreate() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
       
        int sz = 512;
        char p1[sz];
        char p2[sz];
        char p3[sz];
        p1[0] = p2[0] = p3[0] = '\0';

        op_t ops[] = {
            op_create("/multi1", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_create("/multi1/a", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p2, sz),
            op_create("/multi1/b", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p3, sz)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        CPPUNIT_ASSERT(strcmp(p1, "/multi1") == 0);
        CPPUNIT_ASSERT(strcmp(p2, "/multi1/a") == 0);
        CPPUNIT_ASSERT(strcmp(p3, "/multi1/b") == 0);

        CPPUNIT_ASSERT_EQUAL(results[0].err, 0);
        CPPUNIT_ASSERT_EQUAL(results[1].err, 0);
        CPPUNIT_ASSERT_EQUAL(results[2].err, 0);
    }

    /**
     * Test create followed by delete 
     */
    void testCreateDelete() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int sz = 512;
        char p1[sz];
        p1[0] = '\0';
       
        op_t ops[] = {
            op_create("/multi2", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_delete("/multi2", 0)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        // '/multi' should have been deleted
        rc = zoo_exists(zk, "/multi2", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
    }

    /** 
     * Test invalid versions
     */
    void testInvalidVersion() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
       
        op_t ops[] = {
            op_create("/multi3", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0),
            op_delete("/multi3", 1)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZBADVERSION, rc);
    }

    /**
     * Test nested creates that rely on state in earlier op in multi
     */
    void testNestedCreate() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int sz = 512;
        char p1[sz];
        p1[0] = '\0';

        op_t ops[] = {
            /* Create */
            op_create("/multi4", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_create("/multi4/a", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_create("/multi4/a/1", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            
            /* Delete */
            op_delete("/multi4/a/1", 0),
            op_delete("/multi4/a", 0),
            op_delete("/multi4", 0)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        // Verify tree deleted
        rc = zoo_exists(zk, "/multi4/a/1", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
        
        rc = zoo_exists(zk, "/multi4/a", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
  
        rc = zoo_exists(zk, "/multi4", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
    }

    /**
     * Test setdata functionality
     */
    void testSetData() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int sz = 512;
        struct Stat s1;

        char buf[sz];
        int blen = sz ;

        char p1[sz], p2[sz];

        op_t ops[] = {
            op_create("/multi5",   "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_create("/multi5/a", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p2, sz)
        };
        int nops = sizeof(ops) / sizeof(ops[0]) ;
        op_result_t results[nops];
       
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        
        yield(zk, 5);

        op_t setdata_ops[] = {
            op_setdata("/multi5",   "1", 1, 0, &s1),
            op_setdata("/multi5/a", "2", 1, 0, &s1)
        };

        int nsops = sizeof(setdata_ops) / sizeof(setdata_ops[0]);;
        op_result_t setdata_results[nsops];

        rc = zoo_multi(zk, nsops, setdata_ops, setdata_results);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT_EQUAL(results[0].err, 0);
        CPPUNIT_ASSERT_EQUAL(results[1].err, 0);
        
        memset(buf, '\0', blen);
        rc = zoo_get(zk, "/multi5", 0, buf, &blen, &s1);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        CPPUNIT_ASSERT_EQUAL(1, blen);
        CPPUNIT_ASSERT(strcmp("1", buf) == 0);
        
        memset(buf, '\0', blen);
        rc = zoo_get(zk, "/multi5/a", 0, buf, &blen, &s1);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        CPPUNIT_ASSERT_EQUAL(1, blen);
        CPPUNIT_ASSERT(strcmp("2", buf) == 0);
    }

    /**
     * Test update conflicts
     */
    void testUpdateConflict() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int sz = 512;
        char buf[sz];
        int blen = sz;
        char p1[sz];
        p1[0] = '\0';
        struct Stat s1;

        op_t ops[] = {
            op_create("/multi6", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_setdata("/multi6", "X", 1, 0, &s1),
            op_setdata("/multi6", "Y", 1, 0, &s1)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZBADVERSION, rc);

        //Updating version solves conflict -- order matters
        ops[2].version = 1;
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
       
        memset(buf, 0, sz);
        rc = zoo_get(zk, "/multi6", 0, buf, &blen, &s1);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        CPPUNIT_ASSERT_EQUAL(blen, 1);
        CPPUNIT_ASSERT(strncmp(buf, "Y", 1) == 0);
    }

    /**
     * Test delete-update conflicts
     */
    void testDeleteUpdateConflict() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int sz = 512;
        char buf[sz];
        int blen;
        char p1[sz];
        p1[0] = '\0';
        struct Stat stat;

        op_t ops[] = {
            op_create("/multi7", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_delete("/multi7", 0),
            op_setdata("/multi7", "Y", 1, 0, &stat)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);

        // '/multi' should never have been created as entire op should fail
        rc = zoo_exists(zk, "/multi7", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
    }

    void testAsyncMulti() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
       
        int sz = 512;
        char p1[sz], p2[sz], p3[sz];
        p1[0] = '\0';
        p2[0] = '\0';
        p3[0] = '\0';

        op_t ops[3] = {
            op_create("/multi8",   "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_create("/multi8/a", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p2, sz),
            op_create("/multi8/b", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p3, sz)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
 
        rc = zoo_amulti(zk, nops, ops, results, multi_completion_fn, 0);
        waitForMultiCompletion(10);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        CPPUNIT_ASSERT(strcmp(p1, "/multi8") == 0);
        CPPUNIT_ASSERT(strcmp(p2, "/multi8/a") == 0);
        CPPUNIT_ASSERT(strcmp(p3, "/multi8/b") == 0);

        CPPUNIT_ASSERT_EQUAL(results[0].err, 0);
        CPPUNIT_ASSERT_EQUAL(results[1].err, 0);
        CPPUNIT_ASSERT_EQUAL(results[2].err, 0);
    }

    void testMultiFail() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
       
        int sz = 512;
        char p1[sz], p2[sz], p3[sz];

        p1[0] = '\0';
        p2[0] = '\0';
        p3[0] = '\0';

        op_result_t results[3] ;
        op_t ops[3] = {
            op_create("/multi9",   "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz),
            op_create("/multi9",   "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p2, sz),
            op_create("/multi9/b", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p3, sz),
        };
        
        rc = zoo_multi(zk, 3, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZNODEEXISTS, rc);
    }
    
    /**
     * Test basic multi-op check functionality 
     */
    void testCheck() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int sz = 512;
        char p1[sz];
        p1[0] = '\0';
        
        rc = zoo_create(zk, "/multi0", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        // Conditionally create /multi0/a' only if '/multi0' at version 0
        op_t ops[] = {
            op_check("/multi0", 0, 0),
            op_create("/multi0/a", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz)
        };
        int nops = sizeof(ops) / sizeof(ops[0]);
        op_result_t results[nops];
        
        rc = zoo_multi(zk, nops, ops, results);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        CPPUNIT_ASSERT_EQUAL(0, results[0].err);
        CPPUNIT_ASSERT_EQUAL(0, results[1].err);

        // '/multi0/a' should have been created as it passed version check
        rc = zoo_exists(zk, "/multi0/a", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
 
        // Only create '/multi0/b' if '/multi0' at version 10 (which it's not)
        op_t ops2[] = {
            op_check("/multi0", 10, 0),
            op_create("/multi0/b", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, p1, sz)
        };
        
        rc = zoo_multi(zk, nops, ops2, results);
        CPPUNIT_ASSERT_EQUAL((int)ZBADVERSION, rc);

        CPPUNIT_ASSERT_EQUAL((int)ZBADVERSION, results[0].err);
        CPPUNIT_ASSERT_EQUAL((int)ZRUNTIMEINCONSISTENCY, results[1].err);

        // '/multi0/b' should NOT have been created
        rc = zoo_exists(zk, "/multi0/b", 0, NULL);
        CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
    }

};

volatile int Zookeeper_multi::count;
const char Zookeeper_multi::hostPorts_multi[] = "127.0.0.1:22181";
CPPUNIT_TEST_SUITE_REGISTRATION(Zookeeper_multi);
