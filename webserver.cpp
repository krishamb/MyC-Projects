
#include <iostream>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <WinSock2.h>
#include <WS2tcpip.h>
#include <Windows.h>
#include <sys/types.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <vector>
#include <sstream>
#include <iterator>
#include <map>
#include <set>
#include <fstream>
#include <stdio.h>
#include <algorithm>
#include <sys/stat.h>

#if defined(WIN32) || defined(WIN64)
// Copied from linux libc sys/stat.h:
#define S_ISREG(m) (((m) & S_IFMT) == S_IFREG)
#define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
#endif

#pragma comment (lib, "Ws2_32.lib")

#define PORT "3490"  // the port users will be connecting to

#define BACKLOG 10	 // how many pending connections queue will hold

static std::vector<std::string> opvec = {  "GET",
										   "PUT",
										   "POST",
										   "DELETE"
										   };

enum class RequestType { GET, PUT, POST, DEL, DEFAULT};
enum class AuthenticateError { BAD_EMPTY_USERNAMEORPASS, BAD_USERNAMEERROR, BAD_PASSWORDERROR, BAD_CHARACTER_USERNAMEORPASSWORD, GOOD_USERPASS};

constexpr uint32_t MINUSERNAMELENGTH =  3;
constexpr uint32_t MAXUSERNAMELENGTH =  8;
constexpr uint32_t MINPASSWORDLENGTH =  8;

std::vector<std::string> _rcvlines(6);

std::map<std::string, std::string> _usermap;

std::set<std::string> _sessionallowed;

std::string _rcvbuffer;
uint32_t    _rcvbuffersize;

using namespace std;

void writeOutputBufferToFile(const char *filename_, const char *);
bool getUsernameAndPassword(std::string &username_, std::string& password_);


/**
* This gets an Internet address, either IPv4 or IPv6
*
* Helper function to make printing easier.
*/
void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}

	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

/**
* Return the main listening socket
*
* Returns -1 or error
*/
SOCKET get_listener_socket(char *port)
{
	SOCKET sockfd;
	struct addrinfo hints, *servinfo, *p;
	const char *yes = "1";
	int rv;

	// This block of code looks at the local network interfaces and
	// tries to find some that match our requirements (namely either
	// IPv4 or IPv6 (AF_UNSPEC) and TCP (SOCK_STREAM) and use any IP on
	// this machine (AI_PASSIVE).

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE; // use my IP

	if ((rv = getaddrinfo(NULL, port, &hints, &servinfo)) != 0) {
		std::cerr << "getaddrinfo: " << gai_strerror(rv) << "\n";
		return -1;
	}

	// Once we have a list of potential interfaces, loop through them
	// and try to set up a socket on each. Quit looping the first time
	// we have success.
	for (p = servinfo; p != NULL; p = p->ai_next) {

		// Try to make a socket based on this candidate interface
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
			p->ai_protocol)) == -1) {
			//perror("server: socket");
			continue;
		}

		// SO_REUSEADDR prevents the "address already in use" errors
		// that commonly come up when testing servers.
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, yes,
			sizeof(int)) == -1) {
			perror("setsockopt");
			closesocket(sockfd);
			freeaddrinfo(servinfo); // all done with this structure
			return -2;
		}

		// See if we can bind this socket to this local IP address. This
		// associates the file descriptor (the socket descriptor) that
		// we will read and write on with a specific IP address.
		if (bind(sockfd, p->ai_addr, static_cast<int>(p->ai_addrlen)) == -1) {
			closesocket(sockfd);
			//perror("server: bind");
			continue;
		}

		// If we got here, we got a bound socket and we're done
		break;
	}

	freeaddrinfo(servinfo); // all done with this structure

							// If p is NULL, it means we didn't break out of the loop, above,
							// and we don't have a good socket.
	if (p == NULL) {
		std::cerr << "webserver: failed to find local address\n";
		return -3;
	}

	// Start listening. This is what allows remote computers to connect
	// to this socket/IP.
	if (listen(sockfd, BACKLOG) == -1) {
		//perror("listen");
		closesocket(sockfd);
		return -4;
	}

	return sockfd;
}

/**
* Send an HTTP response
*
* header:       "HTTP/1.1 404 NOT FOUND" or "HTTP/1.1 200 OK", etc.
* content_type: "text/plain", etc.
* body:         the data to send.
*
* Return the value from the send() function.
*/
int send_response(SOCKET fd, const std::string& header = "", const std::string& content_type = "", const std::string& body = "", const RequestType type = RequestType::DEFAULT, const std::string& info = "")
{
	constexpr int max_response_size = 65536;
	
	std::ostringstream str;
	str << "\n";

	if ( type == RequestType::DEFAULT || type == RequestType::POST )
	{
		if (!header.empty())
			str << "Status: " << header.c_str() << "\n";

		if (!content_type.empty())
			str << "Content-type:" << content_type.c_str() << "\n";

		if (!body.empty())
		{
			str << "\n\n";

			str << "{" << "\n";
			if (type == RequestType::POST)
				str << "\t" << "\"token\": " << "\"" << body.c_str() << "\"" << "\n";
			else
				str << "\t" << "\"error:\"" << "\"" << body.c_str() << "\"" << "\n";
			str << "}" << "\n";
		}
	}
	else if ( type == RequestType::GET || type == RequestType::DEL )
	{
		if (!header.empty())
			str << "Status: " << header.c_str() << "\n";

		if (header == "200 OK")
		{
			if (!info.empty())
				str << "Content-Length: " << info.c_str() << "\n";

			if (!content_type.empty())
				str << "Content-type:" << content_type.c_str() << "\n";

			str << "\n\n";

			str << body.c_str() << "\n";
		}
	}
	else if (type == RequestType::PUT)
	{
		if (!header.empty())
			str << "Status: " << header.c_str() << "\n";

		if (!content_type.empty())
			str << "Location: " << content_type.c_str() << "\n";
	}

	str << "\n";

	const std::string &cstr = str.str();
	int rv = send(fd, cstr.c_str(), static_cast<int>( cstr.length()), 0);

	if (rv < 0) {
		perror("send");
	}

	return rv;
}

// trim from left
inline std::string& ltrim(std::string& s, const char* t = " \t\n\r\f\v\",{}")
{
	s.erase(0, s.find_first_not_of(t));
	return s;
}

// trim from right
inline std::string& rtrim(std::string& s, const char* t = " \t\n\r\f\v\",{}")
{
	s.erase(s.find_last_not_of(t) + 1);
	return s;
}

// trim from left & right
inline std::string& trim(std::string& s, const char* t = " \t\n\r\f\v\",{}")
{
	return ltrim(rtrim(s, t), t);
}

bool isfile(const char* path) {
	struct stat buf;
	stat(path, &buf);
	return S_ISREG(buf.st_mode);
}

bool isdir(const char* path) {
	struct stat buf;
	stat(path, &buf);
	return S_ISDIR(buf.st_mode);
}

bool processHTTPrecv(const char *str_)
{
	std::stringstream ss(str_);
	std::string line;
	
	std::string word;

	_rcvbuffer = str_;

	_rcvlines.clear();

	while (std::getline(ss, line))
	{
		if (line[line.length() - 1] == '\r')
			line.erase(line.length() - 1);
		_rcvlines.push_back(line);
	}

	return _rcvlines.size() > 0 ? true : false;
}

bool processHTTPline(uint32_t linenum_, std::vector<std::string>& tokens_, const char delim = ' ')
{
	uint32_t count = static_cast<uint32_t> (_rcvlines.size());

	if (count < linenum_ || _rcvlines[linenum_].empty())
		return false;

	std::stringstream ss(_rcvlines[linenum_]);
	std::string word;

	while (std::getline(ss, word, delim))
	{
		if (word.empty())
			return false;
		trim(word);
		tokens_.push_back(word);
	}

	return tokens_.size() > 0;
}

bool processHTTPstring(const string& token_, std::vector<std::string>& tokens_, const char delim = ' ')
{

	std::stringstream ss(token_);
	std::string word;

	while (std::getline(ss, word, delim))
	{
		if (word.empty())
			return false;
		trim(word);
		tokens_.push_back(word);
	}

	return tokens_.size() > 0;
}

bool string_is_valid(const std::string &str)
{
	return std::find_if(str.begin(), str.end(),
		[](const char c) { return !(isalnum(c) || (c == ' ')); }) == str.end();
}

AuthenticateError handleregister()
{
	std::string username, password;

	if (!getUsernameAndPassword(username, password))
		return AuthenticateError::BAD_EMPTY_USERNAMEORPASS;

	if (username.size() < MINUSERNAMELENGTH || username.size() > MAXUSERNAMELENGTH)
		return AuthenticateError::BAD_USERNAMEERROR;

	if (password.size() < MINPASSWORDLENGTH)
		return AuthenticateError::BAD_PASSWORDERROR;

	if (!string_is_valid(username) || !string_is_valid(password))
		return AuthenticateError::BAD_CHARACTER_USERNAMEORPASSWORD;

	_usermap[ username ] = password;

	return AuthenticateError::GOOD_USERPASS;
}

bool handlelogin(std::string& session_)
{
	std::string username, password;

	if (!getUsernameAndPassword(username, password))
		return false;

	auto it = _usermap.find(username);

	if (it == _usermap.end())
		return false;

	int iSecret;

	/* initialize random seed: */
	srand(static_cast<unsigned int>(time(nullptr)));

	/* generate secret number between 1 and 10: */
	iSecret = rand();

	std::stringstream str;
	str << iSecret;

	session_ = str.str();

	_sessionallowed.insert(session_);

	return true;
}

bool handlecreatefile(const std::string& filename_)
{
	std::vector<std::string> tokensession(2), tokens(2);

	processHTTPline(7, tokensession, ':');

	if ( tokensession.empty() )
		return false;

	processHTTPstring(tokensession[3], tokens, ' ');

	std::string session = tokens[2];

	if ( _sessionallowed.count(session) != 1 )
		return false;

	tokens.clear();

	const char *cptr = tokensession[3].c_str();

	cptr = cptr + session.size() + 2;

	const char *fptr = filename_.c_str();

#ifdef _WINDOWS_
	if (*fptr == '/')
		fptr++;
#endif

	writeOutputBufferToFile(fptr, cptr);

	return true;
}

int checkfileAndSession(const std::string& filename_)
{
	const char *ptr = filename_.c_str();

#ifdef _WINDOWS_
	ptr++;
#endif
	std::ifstream in(ptr);

	if (in.fail())
		return -1;

	in.close();
	std::vector<std::string> tokensess(2);
	processHTTPline(7, tokensess, ':');

	if (_sessionallowed.count(tokensess[3]) != 1)
		return 0;

	return 1;
}

bool readFile(const string& fullPath_, std::vector<char>& filebuffer_)
{
	const char *ptr = fullPath_.c_str();

#ifdef _WINDOWS_
	ptr++;
#endif
	ifstream fp(ptr, ios::in | ios::binary);

	if (!fp.is_open())
	{
		return false;
	}
	// get length of file and allocate a buffer of that size

	std::streampos begin = fp.tellg();

	fp.seekg(0, ios::end);
	streampos end = fp.tellg();

	size_t fileLength = end - begin;

	fp.seekg(0, ios::beg);

	filebuffer_.resize(static_cast<unsigned int>(fileLength + 1));

	fp.read(filebuffer_.data(), fileLength);
	fp.close();

	return fileLength > 0;
}

int handlegetfile(const std::string& filename_, std::vector<char>& filebuffer_)
{
	int ret = checkfileAndSession(filename_);

	if (ret)
	{
		bool ret1 = readFile(filename_, filebuffer_);
		return ret1;
	}

	return ret;
}

bool readfiles(const std::string& folder_, std::string& filenames_)
{
		std::string search_path =  folder_ + "/*.*";
		WIN32_FIND_DATAA fd;

		string empty = " ";
		HANDLE hFind = ::FindFirstFileA(search_path.c_str(), &fd);
		if (hFind != INVALID_HANDLE_VALUE) {
			do {
				// read all (real) files in current folder
				// , delete '!' read other 2 default folder . and ..
				if (!(fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
				       filenames_ += fd.cFileName + empty ;
				}
			} while (::FindNextFileA(hFind, &fd));
			::FindClose(hFind);
			return true;
		}
		return false;
}

bool handlegetfile(const std::string& filename_, std::string& filenames_)
{
	std::vector<std::string> tokensess(2);
	processHTTPline(1, tokensess, ':');

	if (_sessionallowed.count(tokensess[1]) != 1)
		return 0;

	return readfiles(filename_, filenames_);
}

int handledeletefile(const std::string& filename_)
{
	int ret = checkfileAndSession(filename_);

	if (ret)
	{
		const char *ptr = filename_.c_str();

#ifdef _WINDOWS_
		ptr++;
#endif
		remove(ptr);

		return 1;
	}

	return ret;
}

void writeOutputBufferToFile(const char *filename_, const char *filebuffer_)
{
//	const char *filename1 = "C:\\csv\\ambarish.txt";
	std::cout << "Filename is:" << filename_ << "\n";
	std::ofstream fp(filename_, ios::out | ios::binary | ios::trunc);
	fp.write(filebuffer_, strlen(filebuffer_));
	fp.close();
}

bool getUsernameAndPassword(std::string &username_, std::string& password_)
{
	std::vector<std::string> tokenuser(2);
	std::vector<std::string> tokenpass(2);
	std::vector<std::string> tokenauth(2);

	processHTTPline(7, tokenauth, ',');

	processHTTPstring(tokenauth[2], tokenuser, ':');
	processHTTPstring(tokenauth[3], tokenpass, ':');

	if (tokenuser.empty() || tokenpass.empty())
		return false;

	if ( ( tokenuser[2].find("username") == std::string::npos ) || ( tokenpass[2].find("password") == std::string::npos))
		return false;

	if (tokenuser[3].empty() || tokenpass[3].empty())
		return false;

	username_ = tokenuser[3];
	password_ = tokenpass[3];

	return true;
}

/**
* Handle HTTP request and send response
*/
void handle_http_request(SOCKET fd)
{
	const int request_buffer_size = 65536; // 64K
	char request[request_buffer_size];

								// Read request
	int bytes_recvd = recv(fd, request, request_buffer_size - 1, 0);

	if (bytes_recvd < 0) {
		perror("recv");
		return;
	}

	// NUL terminate request string
	request[bytes_recvd] = '\0';
	_rcvbuffersize = bytes_recvd;

	bool ret = processHTTPrecv(request);

	if (!ret)
	{
		send_response(fd, "HTTP/1.1 404 NOT FOUND", "text/html", "<h1>404 Page Not Found</h1>", RequestType::DEFAULT);
		return;
	}
    
	std::vector<std::string> tokens;
	ret = processHTTPline(0, tokens);

	if (!ret)
	{
		send_response(fd, "HTTP/1.1 404 NOT FOUND", "text/html", "<h1>404 Page Not Found</h1>", RequestType::DEFAULT);
	}
	
	auto it = std::find( opvec.begin(), opvec.end(), tokens[0]);

	if (it == opvec.end())
	{
		send_response(fd, "HTTP/1.1 404 NOT FOUND", "text/html", "<h1>404 Page Not Found</h1>", RequestType::DEFAULT);
	}

	if ( tokens[0] == "GET" )
	{
		std::vector<char> filebuffer;
		std::string		  filestring("[\r\n");

		int ret;
		if (isfile(tokens[1].c_str()))
			ret = handlegetfile(tokens[1], filebuffer);
		else
		{
			string endstr	= "\r\n]\r\n";

			ret = handlegetfile(tokens[1], filestring);
			filestring += endstr;
		}

		if (ret == 0)
		{
			send_response(fd, "403 Forbidden", "", "", RequestType::GET);
		}
		else if (ret == -1)
		{
			send_response(fd, "404 Not Found", "", "", RequestType::GET);
		}
		else if (ret == 1)
		{
			std::ostringstream str1;
			string			   fstr;

			if (!filebuffer.empty())
			{
				str1 << filebuffer.size();
				fstr.insert(fstr.end(), filebuffer.begin(), filebuffer.end());
			}
			else
			{
				fstr = filestring;
			}

			send_response(fd, "200 OK", "text/html", fstr, RequestType::GET, str1.str());
		}
	}
	else if ( tokens[0] == "PUT" )
	{
		bool ret = handlecreatefile( tokens[1] );

		if (ret)
		{
			send_response(fd, "201 Created", tokens[1] , "", RequestType::PUT);
		}
	}
	else if ( tokens[0] == "DELETE")
	{
		int ret = handledeletefile(tokens[1]);

		if (ret == 0)
		{
			send_response(fd, "403 Forbidden", "", "", RequestType::DEL);
		}
		else if (ret == -1)
		{
			send_response(fd, "404 Not Found", "", "", RequestType::DEL);
		}
		else if (ret == 1)
		{
			send_response(fd, "204 No Content", "", "", RequestType::DEL);
		}
	}
	else if (tokens[0] == "POST")
	{
		if (tokens[1] == "/register")
		{
			AuthenticateError ret = handleregister();

			switch (ret)
			{
				case AuthenticateError::GOOD_USERPASS: send_response(fd, "204 No Content");
													   break;

				case AuthenticateError::BAD_EMPTY_USERNAMEORPASS:
													   send_response(fd, "400 BAD REQUEST", "application/jason", "Missing username or password");
													   break;
				case AuthenticateError::BAD_USERNAMEERROR:
													   send_response(fd, "400 BAD REQUEST", "application/jason", \
																		 "Bad Username (Username should be less than 8 and min 3 chars");
													   break;
				case AuthenticateError::BAD_PASSWORDERROR:
													   send_response(fd, "400 BAD REQUEST", "application/jason", \
																	"Bad Password ( Password should be min 8 chars");
													   break;
				case AuthenticateError::BAD_CHARACTER_USERNAMEORPASSWORD:
													   send_response(fd, "400 BAD REQUEST", "application/jason", \
																		  "Bad chars ( Only Alphanumeric chars allowed");
													   break;
			}
		}
		else if (tokens[1] == "/login")
		{
			std::string session;
			bool ret = handlelogin(session);
			if (!ret)
			{
				send_response(fd, "400 BAD REQUEST", "application/jason", "Incorrect Username/Password");
			}
			else
			{
				send_response(fd, "200 OK", "application/jason", session, RequestType::POST);
			}
		}
		else
		{
			send_response(fd, "400 BAD REQUEST", "application/jason", "Missing POST requesttype");
		}
	}
}

/**
* Main
*/
int main(void)
{
	SOCKET newfd;  // listen on sock_fd, new connection on newfd
	struct sockaddr_storage their_addr; // connector's address information
	char s[INET6_ADDRSTRLEN];

	// Initialize Winsock
	WSADATA wsaData;
	int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (iResult != 0) {
		std::cerr << "WSAStartup failed with error: " << iResult << "\n";
		return 1;
	}

	// Get a listening socket
	SOCKET listenfd = get_listener_socket(PORT);

	if (listenfd < 0) {
		std::cerr << "webserver: fatal error getting listening socket\n";
		exit(1);
	}

	std::cout << "webserver: waiting for connections...\n";

	// This is the main loop that accepts incoming connections and
	// queues to a handler process to take care of it. The main parent
	// process then goes back to waiting for new connections.

	while (1) {
		socklen_t sin_size = sizeof their_addr;

		// Parent process will block on the accept() call until someone
		// makes a new connection:
		newfd = accept(listenfd, (struct sockaddr *)&their_addr, &sin_size);
		if (newfd == -1) {
			perror("accept");
			continue;
		}

		// Print out a message that we got the connection
		inet_ntop(their_addr.ss_family,
			get_in_addr((struct sockaddr *)&their_addr),
			s, sizeof s);

		std::cout << "server: got connection from: " << s << "\n";

		// newfd is a new socket descriptor for the new connection.
		// listenfd is still listening for new connections.

		// Convert this to be multiprocessed with queueing and threading model

		handle_http_request(newfd);

		// Done with this
		closesocket(newfd);
	}

	// Unreachable code

	return 0;
}