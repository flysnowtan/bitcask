#ifndef __HASHLOCK__H__
#define __HASHLOCK__H__

#include <unistd.h>
#include <fcntl.h>
#include <string>
#include "filelock.h"

class clsHashLock {
	public:
		clsHashLock(clsFileLock *file_lock, uint32_t hash);
		~clsHashLock();
		int ReadLock();
		int WriteLock();

	private:
		clsFileLock *m_FileLock;
		uint32_t m_HashVal;
};

#endif 
