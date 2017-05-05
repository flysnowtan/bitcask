CPP = g++ -m32 -g
CC  = gcc -m32 -g

CPPSRCS  = $(wildcard *.cpp)
CSRCS  = $(wildcard *.c)

CPPOBJS  = $(patsubst %.cpp,%.o,$(CPPSRCS))
COBJS  = $(patsubst %.c,%.o,$(CSRCS))

SRCS = $(CPPSRCS) $(CSRCS)
OBJS = $(CPPOBJS) $(COBJS)

BUILDEXE = $(CPP) -fPIC $(BFLAGS) -o $@ $^ 

bitcask: $(SRCS)
	$(BUILDEXE)

