#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

static pid_t start_kfs(void)
{
	pid_t pid = fork();
	if(pid != 0)
	{
		if(pid > 0 && setpgid(pid, pid) != 0 && errno != EACCES)
			perror("setpgid(kfs_main)");
		return pid;
	}

	if(setpgid(0, 0) != 0)
	{
		perror("setpgid(kfs_main child)");
		_exit(127);
	}
	execlp("sudo", "sudo", "../../build/kfs_main", (char*)NULL);
	perror("exec kfs_main");
	_exit(127);
}

static int stop_kfs(pid_t pid)
{
	int status;
	if(kill(-pid, SIGTERM) != 0 && errno != ESRCH)
	{
		perror("signal kfs_main process group");
		return -1;
	}
	while(waitpid(pid, &status, 0) < 0)
	{
		if(errno != EINTR)
		{
			perror("wait for kfs_main");
			return -1;
		}
	}
	return 0;
}

void get_new_cmd(char new_cmd[], const char* cmd, const char* argv[], int argv_num)
{
	int cmdlen = strlen(cmd);
	// for(int i = 0; i < 1024; i++)
	// {
	// 	if(cmd[i] != 0 && cmd[i] != '\n')
	// 		cmdlen++;
	// 	else
	// 		break;
	// }
	// printf("cmdlen: %d\n",cmdlen);
	int cur = 0;
	for(int i = 0; i < cmdlen; i++)
		new_cmd[i] = cmd[i];
	cur = cmdlen;
	for(int i = 1; i <= argv_num; i++)
	{
		int len = strlen(argv[i]);
		fprintf(stderr,"argv[%d]: %s\n",i,argv[i]);
		new_cmd[cur++] = ' ';
		for(int j = 0; j < len; j++)
		{
			new_cmd[cur] = argv[i][j];
			cur += 1;
		}
	}
	new_cmd[cur++] = 0;
	assert(strlen(new_cmd) == (size_t)(cur - 1));
}

int main(int argc, const char* argv[]) 
{ 
	if(argc != 7)
	{
		fprintf(stderr,
			"usage: %s <block-size> <fs-name> <run-id> <path> <work-type> <threads>\n",
			argv[0]);
		return 2;
	}
	// const char* cmd = "sudo taskset -c 0-27,56-83 ./run_fs2.sh";
	const char* cmd = "sudo sh run_fs2.sh";
	char new_cmd[1024] = {0};

	get_new_cmd(new_cmd, cmd, argv, 6);

	printf("cmd: %s\n",new_cmd);

	if(system("sudo ../../build/mkfs") != 0)
		return 1;

	pid_t kfs_pid = start_kfs();
	if(kfs_pid < 0)
	{
		perror("start kfs_main");
		return 1;
	}
	sleep(8);
	int workload_status = system(new_cmd);
	sleep(1);
	printf("will close kernelFS pid %d\n", kfs_pid);
	if(stop_kfs(kfs_pid) != 0)
		return 1;
	return workload_status == 0 ? 0 : 1;
}
