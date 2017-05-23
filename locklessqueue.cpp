#include <iostream>
#include <string>
#include <functional>

template <typename T>
struct LocklessQueue 
{
private:
	struct Node 
	{
    		Node( T* val ) : value_(val), next_(nullptr) { }
    		T* value_;
    		std::atomic<Node*> next_;
    		char pad_[CACHE_LINE_SIZE - sizeof(T*)- sizeof(atomic<Node*>)];
 	};

	char pad0_[CACHE_LINE_SIZE];
 
	// for one receiver at a time
	Node* first_;
 
	char pad1_[CACHE_LINE_SIZE - sizeof(Node*)];
 
	// shared among multiple receivers
	atomic<bool> rcvLock_;
 
	char pad2_[CACHE_LINE_SIZE - sizeof(atomic<bool>)];
 
	// for one producer at a time
	Node* last_; 
 
	char pad3_[CACHE_LINE_SIZE  - sizeof(Node*)];
 
	// shared among producers
	atomic<bool> producerLock_;
 
	char pad4_[CACHE_LINE_SIZE - sizeof(atomic<bool>)];

public:
	bool ReceiveCallback(T& data)
 	{
    	 while ( rcvLock_.exchange(true) ) 
		{}
      	 Node* first = first_;
     	 Node* next = first_->next_;
     	 if (next != NULL) {            // if queue is nonempty
         	T* val = next->value_;     // take it out
         	next->value_ = NULL;       // of the Node
         	first_ = next;             // swing first forward
         	rcvLock_ = false;
         	data = *val;               // now copy it back
         	delete val;                // clean up the value
         	delete first;              // and the old dummy
         	return true;               // and report success
     	}
     	rcvLock_ = false;

     	return false;                  // report queue was empty 
};

struct MessageSource
{
       std::function<bool(const std::string& msg)> callback_;

       template<typename A, typename B>
       void connect(A func_ptr, B obj_ptr)
       {
              callback_ = std::bind(func_ptr, obj_ptr, placeholders::_1);
       }

       void send_msg(const string& msg)
       {
		// Enqueue the msg
       		if (callback_) {
		     std::string str;
                     callback_(str);
		}
       }

       void disconnect()
       {
       		callback_ = nullptr;
       }
};

main()
{
	LocklessQueue	p;
	MessageSource	s;

	s.connect(&LocklessQueue::ReceiveCallback, &p);
	s.send_msg("msg1 to queue");
	s.disconnect();
	s.send_msg("msg2 to queue"); // this msg remains in the queue till callback is registered
}