
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <algorithm>
#include <set>
#include <queue>
#include <list>
#include <unordered_map>
#include <assert.h>
#include <stack>
#include <iterator>

#include "tradecsv.h"

using namespace std;
using namespace tradecsv;

void EXPECT(const std::string& testname_, int actual_, int expected_);

int TEST1(const std::string& filename_);
int TEST2(const std::string& filename_);
int TEST3(const std::string& filename_);

int main()
{
	string filename;

	std::cout << "Enter input filename...\n";
	std::cin >> filename;

	if (filename.empty())
		return 0;

	size_t sep = filename.find_last_of("\\");

	string outputfile;
	if (sep != string::npos)
	{
		outputfile = filename.substr(0, sep + 1) + "output.csv";
	}

	cout << "Outputfilename is: " << outputfile << "\n";

	int ret = TEST1(filename);
	EXPECT("TEST1", ret, 0);

	ret = TEST2(filename);
	EXPECT("TEST2", ret, 0);

	ret = TEST3(filename);
	EXPECT("TEST3", ret, 0);
}

void EXPECT(const std::string& testname_, int actual_, int expected_)
{
	if (actual_ == expected_)
		std::cout << testname_ << " PASSED\n";
	else
		std::cout << testname_ << " FAILED\n";
}

int TEST1(const std::string& filename_)
{
	tradecsv::CsvParser myparser(filename_, 1);
	if (!myparser.readFile())
	{
		std::cerr << "File: " << filename_ << " cannot be read\n";
		return -1;
	}

	myparser.writeOutputFile();

	return 0;
}

int TEST2(const std::string& filename_)
{
	tradecsv::CsvParser myparser(filename_, 1);
	myparser.setSystemMemoryBytes(200000);

	std::cout << "Setting system memory limit to 200000...\n";

	if (!myparser.readFile())
	{
		std::cerr << "File: " << filename_ << "cannot be read\n";
		return -1;
	}

	myparser.writeOutputFile();

	return 0;
}

int TEST3(const std::string& filename_)
{	
	size_t numfilechunks;
	std::cout << "Please input number of chunks of files to process like source_1, source_2, source_3 .. etc\n";
	std::cout << "Please split source file into multiple chunks since hard disk dont have space\n";
	std::cout << "There is a delay of 3 mins to process between each files in order to get the next chunk of the source\n";

	std::cin >> numfilechunks;

	
	tradecsv::CsvParser myparser(filename_, numfilechunks);
	if (!myparser.readFile())
	{
			std::cerr << "Source Files: " << filename_ << "cannot be read\n";
			return -1;
	}
	
	myparser.writeOutputFile();

	return 0;
}