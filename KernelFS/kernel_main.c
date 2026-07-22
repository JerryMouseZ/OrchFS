#include "kernel_func.h"
#include "balloc.h"
#include "spdk_device_service.h"
#include "spdk_nvme_bridge.h"
#include "orchfs/repro_trace.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t stop_requested;
static volatile sig_atomic_t snapshot_requested;
static unsigned snapshot_sequence;

static void request_stop(int signal_number)
{
    (void)signal_number;
    stop_requested = 1;
}

static void request_snapshot(int signal_number)
{
    (void)signal_number;
    snapshot_requested = 1;
}

int main(void)
{
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = request_stop;
    sigemptyset(&action.sa_mask);
    if(sigaction(SIGINT, &action, NULL) != 0 ||
       sigaction(SIGTERM, &action, NULL) != 0)
    {
        perror("install KFS signal handler");
        return 1;
    }
    action.sa_handler = request_snapshot;
    if(sigaction(SIGUSR1, &action, NULL) != 0)
    {
        perror("install KFS snapshot signal handler");
        return 1;
    }

    init_kernelFS();
    const int durability = orchfs_spdk_device_effective_write_durability();
    const char *durability_name =
        durability == ORCHFS_SPDK_DURABILITY_COMPLETION ? "completion" :
        durability == ORCHFS_SPDK_DURABILITY_FUA ? "fua" :
        durability == ORCHFS_SPDK_DURABILITY_FLUSH ? "flush" : "auto";
    fprintf(stderr, "OrchFS SPDK durability=%s volatile_write_cache=%d\n",
            durability_name,
            orchfs_spdk_device_volatile_write_cache_present());
    if(orchfs_repro_space_snapshot("server_ready") != 0)
        perror("record initial OrchFS allocation snapshot");
    fprintf(stderr, "OrchFS coroutine server is ready\n");
    while(!stop_requested)
    {
        pause();
        if(snapshot_requested)
        {
            char label[64];
            snapshot_requested = 0;
            snprintf(label, sizeof(label), "signal-%u", ++snapshot_sequence);
            if(orchfs_repro_space_snapshot(label) != 0)
                perror("record OrchFS allocation snapshot");
        }
    }

    if(orchfs_repro_space_snapshot("server_stop") != 0)
        perror("record final OrchFS allocation snapshot");
    close_kernelFS();
    orchfs_repro_trace_flush();
    return 0;
}
