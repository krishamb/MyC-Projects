#ifndef _SOCKET_H

#define _SOCKET_H
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>    

namespace sock {
    
    /***************Abstract Socket class**************/
    
    class Socket
    {
     public:
    
     	enum ProtocolType {
        	PROTO_TCP,
       	 	PROTO_UDP
      	};
   
      	Socket();
    
      	virtual ~Socket() = 0;
    
      	static void close( int socket );
    
      	int port( void ) const { return port_; }

	string& ipaddr(void) const { return ip_addr_; }
    
     	static bool isValid( int socket ) { return socket != -1; }
    
      	static void setBlocking( int socket, bool enable );
    
      	virtual int writeBuffer(register const void *buffer, register long bufferSize,int flag = 0) = 0; // This is to write the buffer to socket to be implemented by derived
    
     	virtual int readBuffer(register void *buffer, register long bufferSize,int flag = 0) = 0; / This is to read from socket to the buffer to be implemented by derived

	virtual bool conncreateServer() = 0; // This is to create a socket endpoint for communication

	virtual int connaccept(int socket) = 0;  // This is for TCP Server accept blocking/non blocking with select for TCP Client to send msgs, return new socket descriptor

	virtual bool conncreateClient() = 0; // This is for TCP Client and UDP Client. TCP Client just does connect system call and UDP Client just binds so that server can send back msgs

	void setserveraddr(sockaddr pserv_addr)   { pserv_addr_ = pserv_addr ; }
	const sockaddr& getserveraddr()		    { return pserv_addr_; }

	void setclientaddr(sockaddr pclient_addr) { pclient_addr_ = pclient_addr; }
	const sockaddr& getclientaddr() 	    { return pclient_addr_; }
    
     protected:
    
      	int csocket_;
	int ssocket_;
      	int port_;
	string ip_addr_;
	struct sockaddr pserv_addr_,pclient_addr_;

	static constexpr char CLIENT = '1';
	static constexpr char SERVER = '2';
    };
    
} // sock namespace
    
#endif


#include "Socket.h" // above file

// Pure virtual destructor
Socket::~Socket()
{
    if(csocket_ != INVALID_SOCKET)
    {
        closesocket(csocket_);
    }

    if(ssocket_ != INVALID_SOCKET)
    {
	closesocket(ssocket_);
    }
}

bool Socket::isClientValid()
{
    return csocket_ != INVALID_SOCKET;
}

bool Socket::isServerValid()
{
    return ssocket_ != INVALID_SOCKET;
}

// Accessor for SOCKET member
int Socket::getclientSocket()
{
     return csocket_;
}

// Accessort for SOCKET member
int Socket::getserverSocket()
{
     return ssocket_;
}

// Accessor for SOCKET member
int Socket::getSocket(char id)
{
     return id == CLIENT ? getclientsocket():getserversocket();
}


// Custom constructor
Socket::Socket(int csocket,int ssocket) : csocket_(csocket), ssocket_(ssocket)
{
}

Socket::Socket(const string& ipaddr, int port):ipaddr_(ipaddr),port_(port) {}


// Default constructor (hidden)
Socket::Socket()
{
}

class TCPSocket:public Socket
{
 public:
       TcpSocket(const string& ipaddr,int port)::Socket(ipaddr,port);

       virtual bool conncreateServer() 
	{
	    if ( (ssocket_ = socket(AF_INET,SOCK_STREAM,0)) < 0 ) {
		cerr << "TCPServer: can't open stream socket" << "\n";
		return false;
	    }

	    struct sockaddr_in serv_addr;

	    bzero((char *)&serv_addr,sizeof(serv_addr));
	    serv_addr.sin_family      = AF_INET;
	    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	    serv_addr.sin_port 	      = htons(port());

	    if (bind(ssocket_, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
		cerr << "TCPserver: can't bind local address" << "\n";
		return false;
	    }

	    listen(ssocket_, 5);

	    return true;
	}

	virtual bool conncreateClient()
	{	
	    struct sockaddr_in serv_addr;

	    if ( (cclient_ = socket(AF_INET,SOCK_STREAM,0)) < 0 ) {
		cerr << "TCPClient: can't open stream socket" << "\n";
		return false;
	    }

	    bzero((char *)&serv_addr,sizeof(serv_addr));
	    serv_addr.sin_family      = AF_INET;
	    serv_addr.sin_addr.s_addr = inet_addr(ipaddr().c_str());
	    serv_addr.sin_port 	      = htons(port());

	    if ( connect(csocket_,(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0 ) {
               cerr << "TCPClient:conncreateClient(), can't connect to server:" << ipaddr().c_str() << " \n";
	       return false;
	    }

	    setserveraddr(std::move(serv_addr));	    
	}

	virtual int readBuffer(char id,register void *buffer, const register long bufferSize,int flags)
	{
		long nleft, nread;
		
		nleft = bufferSize;

		while (nleft > 0) {
			nread = read(getsocket(id),buffer,nleft);

			if ( nread < 0 )
				return (nread);
			else if ( nread == 0 )
				break; // EOF

			nleft -= nread;
			ptr   += nread;
		}		
		
		return (bufferSize - nleft);
	}

	virtual int writeBuffer(char id, register const void *buffer, const register long bufferSize,int flags)
	{
		long nleft, nwritten;

		nleft = bufferSize;

		while(nleft > 0) {
			nwritten = write(getsocket(id),buffer,nleft);
			if (nwritten <= 0)
				return (nwritten);

			nleft -= nwritten;
			ptr   += nwritten;
		}

		return (bufferSize - nleft);

	}
};

class UDPSocket:public Socket
{
  public:
	UDPSocket(const string &ipaddr, int port):Socket(ipaddr,port)
	bool virtual conncreateServer() 
	{
	 	if ( (ssocket_ = socket(AF_INET,SOCK_DGRAM,0)) < 0 ) {
			cerr << "UDPServer: can't open stream socket" << "\n";
			return false;
	    	}

		struct sockaddr_in serv_addr;
		bzero((char *)&serv_addr,sizeof(serv_addr));

	    	serv_addr.sin_family      = AF_INET;
	    	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	    	serv_addr.sin_port 	  = htons(port());

		if (bind(ssocket_, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
			cerr << "server: can't bind local address" << "\n";
			return false;
	    	}

		return true;
	}


	virtual bool conncreateClient()
	{	
	    struct sockaddr_in client_addr;

	    bzero((char *)&client_addr,sizeof(serv_addr));
	    client_addr.sin_family      = AF_INET;
	    client_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	    client_addr.sin_port 	= htons(0);

	    if ( (sclient_ = socket(AF_INET,SOCK_DGRAM,0)) < 0 ) {
			cerr << "UDPClient: can't open stream socket" << "\n";
			return false;
	    }


	    if ( connect(sclient_,(struct sockaddr *)&client_addr,sizeof(client_addr)) < 0 ) {
               cerr << "TCPClient:conncreateClient(), can't connect to server" << "\n";
	       return false;
	    }

	    setclientaddr(std::move(client_addr));	    
	}



	virtual int readBuffer(char id, register void *buffer, const register long bufferSize, int flags)
	{
		struct	sockaddr pcli_addr;
		long	bufSiz = bufferSize;
		
		if ( bufferSize > MAXMSGSIZE ) {
			cerr << "UDPSocket: readBuffer recvd Buffersize > MAXMSGSIZE" << "\n";
			bufSiz = MAXMSGSIZE;
		}

		int n = recvfrom(getsocket(id), buffer, bufSiz, 0, &pcli_addr, sizeof(pcli_addr));

		if ( n < 0 ) {
			cerr << "UDPSocket: recvfrom error" << "\n";
			return 0;
		}

		setclientaddr(std::move(pcli_addr));

		return n;						
	}


	virtual int writeBuffer(char id, register const void *buffer, const register long bufferSize, int flags)
	{
		struct sockaddr *pserv_addr = getserveraddr();
		if(send(getsocket(id), buffer, bufferSize, 0, pserv_addr, sizeof(*pserv_addr)) != bufferSize) {
                      cerr << "UDPSocket: sendto error on socket" << "\n";
		      return 0;
		}

		return bufferSize;
	}

  private:
        static constexpr int MAXMSGSIZE = 2048;
}

}
