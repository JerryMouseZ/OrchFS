#include "kernel_func.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t stop_requested;

static void request_stop(int signal_number)
{
    (void)signal_number;
    stop_requested = 1;
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

    init_kernelFS();
    fprintf(stderr, "OrchFS coroutine server is ready\n");
    while(!stop_requested)
        pause();

    close_kernelFS();
    return 0;
}
