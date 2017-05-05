#include <iostream>
#include <time.h>
#include <string>
#include "bitcask.h"

using namespace std;

int main(void)
{
	BitCask cask;
//	int ret = 0;
	int ret = cask.Init();
	if(ret != 0) {
		cout << "err: ret " << ret << endl;
		return 0;
	}

	srand(time(NULL));
	for(int i = 0; i < 100000; i++) {
		int rad = rand() % 20;
		/*
		int rad = 1000;
		char tmp[20] = {0};
		snprintf(tmp, 20, "%d", rad);
		string val = "valdddd";
		val += tmp;
		ret = cask.Add(rad, val);
		if(ret != 0) {
			cout << "cask.add fail , ret " << ret <<endl;
			return 0;
		}
		*/

		if(i % 4 == 0) {
			char tmp[20] = {0};
			snprintf(tmp, 20, "%d", rand());
			string val = "val145ddd6";
			val += tmp;
			ret = cask.Add(rad, val);
			if(ret != 0) {
				cout << "cask.add fail , ret " << ret <<endl;
				return 0;
			}
		} else if(i % 4 == 1) {
			string val;
			ret = cask.Get(rad, val);
			if(ret != 0 && ret != 1) {
				cout << "cask.get fail, ret " << ret <<endl;
				return 0;
			} else if( ret == 1) {
				printf("not found, key %d\n", rad);
			} else {
				printf("find key %d, value %s\n", rad, val.c_str());
			}
		} else if(i % 4 == 2) {
			char tmp[20] = {0};
			snprintf(tmp, 20, "%d", rand());
			string val = "val145ddd6";
			val += tmp;
			ret = cask.Set(rad, val);
			if(ret != 0) {
				cout << "cask.add fail , ret " << ret <<endl;
				return 0;
			}
		} else if(i % 4 == 3) {
			ret = cask.Delete(rad);
			if(ret != 0 && ret != 1) {
				cout << "cask.delete fail, ret " << ret <<endl;
				return 0;
			} else if(ret == 1) {
				printf("delete not found, key %d\n", rad);
			}
		}
//		usleep(30000);
	}

	return 0;
}