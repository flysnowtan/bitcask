#pragma once
#include <unistd.h>
#include <fcntl.h>
#include <string>

class clsFileLock {
	public:
		clsFileLock(const std::string& filename);
		virtual ~clsFileLock();
		int lockR(int offset, int len);
		int lockW(int offset, int len);
		int lockRBlock(int offset, int len);
		int lockWBlock(int offset, int len);
		int unlock(int offset, int len);
	private:
		int _lock(int offset, int len, int type, int cmd);
		int _fd;
};

