#include "filelock.h"
#include <stdio.h>
#include <stdlib.h>

clsFileLock::clsFileLock(const std::string& filename)
{
	_fd = open(filename.c_str(), O_RDWR | O_CREAT, 0600);

	if(_fd < 0)
	{
		printf("failed to open file: %s\n", filename.c_str());
		exit(1);
	}
}

clsFileLock::~clsFileLock()
{
	if(_fd > 0)
	{
		close(_fd);
	}
}

int clsFileLock::lockR(int offset, int len)
{
	return _lock(offset, len, F_RDLCK, F_SETLK);
}

int clsFileLock::lockW(int offset, int len)
{
	return _lock(offset, len, F_WRLCK, F_SETLK);
}

int clsFileLock::lockRBlock(int offset, int len)
{
	return _lock(offset, len, F_RDLCK, F_SETLKW);
}

int clsFileLock::lockWBlock(int offset, int len)
{
	return _lock(offset, len, F_WRLCK, F_SETLKW);
}

int clsFileLock::unlock(int offset, int len)
{
	return _lock(offset, len, F_UNLCK, F_SETLK);
}

int clsFileLock::_lock(int offset, int len, int type, int cmd)
{
	struct flock stFlock;
	stFlock.l_type = type;
	stFlock.l_whence = SEEK_SET;
	stFlock.l_start = offset;
	stFlock.l_len = len;

	int iRet;
	iRet = fcntl(_fd, cmd, &stFlock);
	if(iRet < 0)
	{
		perror("err:");
	}
	return iRet;
}
