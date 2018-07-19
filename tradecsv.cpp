/* this function reads the CSV-file into a buffer that is allocated , this function first checks the size of the file, then allocates a buffer which it puts the file contents
@param none
@result true if something was read. */
#include "tradecsv.h"
#include <sstream>

namespace tradecsv {

	std::ofstream& operator<<(std::ofstream& os_, const std::pair<std::string, Trade>& trade_)
	{
		os_ << trade_.first << "," << trade_.second.maxtimegap() << "," << trade_.second.volume() << "," << trade_.second.weight() << trade_.second.maxprice() << "\n";
		return os_;
	}

	CsvParser::CsvParser(const std::string& fullPath, size_t filechunksize_) : _filename(fullPath), _numberoffilechunks(filechunksize_)
	{
		size_t sep = _filename.find_last_of("\\");

		if (sep != std::string::npos)
		{
			_outputfile = _filename.substr(0, sep + 1) + "output.csv";
		}
		
		_numbytessystem = getTotalSystemMemory();
	}

	CsvParser::~CsvParser() {  }

	bool CsvParser::getFileSize(size_t& filesize_)
	{
		_fp.open(_filename.c_str(), std::ios::in | std::ios::binary);

		if (!_fp.is_open())
		{
			return false;
		}
		// get length of file and allocate a buffer of that size

		std::streampos begin = _fp.tellg();

		_fp.seekg(0, std::ios::end);
		std::streampos end = _fp.tellg();

		auto _fileLength = end - begin;

		_fp.seekg(0, std::ios::beg); //rewind to beginning

		filesize_ = _fileLength;

		return _fileLength > 0;
	}

	bool CsvParser::determineChunkSizeforfile()
	{
		size_t filesize;

		if (!getFileSize( filesize ) )
			return false;

		unsigned long long memoryavailable = getSystemMemoryBytes();

		if ( filesize < memoryavailable )
		{
			_chunksize		= filesize;
			_lastchunksize	= 0;
			_totalchunks	= 1;
		}
		else
		{
			_totalchunks   = filesize / memoryavailable;
			_lastchunksize = filesize % memoryavailable;
			_chunksize	   = memoryavailable; // This can be changed to any size
		}

		if(_lastchunksize != 0) /* if the above division was uneven */
		{
			++_totalchunks; /* add an unfilled final chunk */
		}
		else /* if division was even, last chunk is full */
		{
			_lastchunksize = _chunksize;
		}

		std::cout << "using chunk size: " << _chunksize << " total chunks: " << _totalchunks << "\n";

		return _chunksize > 0;
	}

	void CsvParser::AllocateBuffer()
	{
		_fileBuffer.resize(static_cast<unsigned int>(_chunksize + 1));
	}

	uint32_t CsvParser::readFile()
	{
		uint32_t total_lines = 0;
		size_t   numfilechunks = 0;

		numfilechunks = getNumfileChunks();

		for (auto ch = 0; ch < numfilechunks; ch++)
		{
			if (numfilechunks > 1)
			{
				_filename = getfilenamebase() + "_" + std::to_string(ch + 1) + "." + getfilenameext() ;
				if ( ch > 1 )
					Sleep(60 * 1000); // Sleep for a minute to get the file
			}
			if (!determineChunkSizeforfile())
				return 0;

			for (size_t chunk = 0; chunk < _totalchunks; ++chunk)
			{
				size_t this_chunk_size =
					chunk == _totalchunks - 1 /* if last chunk */
					? _lastchunksize /* then fill chunk with remaining bytes */
					: _chunksize; /* else fill entire chunk */
				AllocateBuffer();
				_fp.read(&_fileBuffer[0], this_chunk_size);

				/* do something with chunk_data before next iteration */
				std::cout << "Parsing chunk # " << chunk << " filename: " << _filename << "\n";

				total_lines += parseChunk();

				_fileBuffer.clear();

#ifdef DUMPFILE
				for (const auto c : _fileBuffer) /* I like my C++11 extensions */
				{
					std::cout << c;
				}

				std::cout << "\n";
#endif
			}
		}
		writeOutputFile();

		return total_lines;
	}

	void CsvParser::writeOutputFile()
	{
		std::ofstream fp(_outputfile, std::ios::out | std::ios::trunc);

		for (auto& trade : _trademap)
			fp << trade;

		fp.close();
	}

	uint32_t CsvParser::parseChunk()
	{
		std::string str(_fileBuffer.begin(), _fileBuffer.end());
		std::stringstream ss(str);
		std::string line;
		char   delim = ',';
		static uint32_t linecount = 0;
		std::vector<uint64_t> _csvvalues;
		bool   chunkend = false;

		_csvvalues.reserve(3);

		while (std::getline(ss, line))
		{
			if (line.empty() || line[0] == NULL )
				continue;

			if (line.find(',') == std::string::npos)
			{
				chunkend = true;
				break;
			}

			if (!_prevchunklastdata.empty())
			{
				if (_prevchunklastdata[ _prevchunklastdata.length() - 1 ] == '\0')
					_prevchunklastdata.erase(_prevchunklastdata.length() - 1);

				line = _prevchunklastdata + line;
				_prevchunklastdata = "";
			}

			std::stringstream	ssl(line);
			short				count = DataType::TIME;

			std::string		symbol;
			std::string		word;
		
			short col = 0;

			_csvvalues.clear();
			while (getline(ssl, word, delim))
			{
				if (word.empty())
					continue;
				trim( word );
			
				if (count != DataType::SYMBOL)
				{
					_csvvalues.push_back(stoull(word));
					++col;
				}
				else
				{
					symbol = word;
				}
				count++;
			}

			if (_csvvalues.size() == 3)
			{
				auto& it = _trademap.find(symbol);
				auto currweightpricenum = _csvvalues[QUANTITY_COL] * _csvvalues[PRICE_COL];

				if (it != _trademap.end())
				{
					auto volume				= it->second.volume();
					auto maxprice			= it->second.maxprice();
					auto maxtimegap			= it->second.maxtimegap();
					auto prevtime			= it->second.prevtime();
					auto weightpricenum		= it->second.weightpricenum();

					auto currtimegap		= _csvvalues[TIME_COL] - prevtime;
		
					it->second.setvolume( volume + _csvvalues[QUANTITY_COL] );
					it->second.setmaxprice( max( maxprice , _csvvalues[PRICE_COL]) );
					it->second.setmaxtimegap( max( maxtimegap, currtimegap ) );
					it->second.setprevtime( _csvvalues[TIME_COL] );
					it->second.setweightpricenum( weightpricenum + currweightpricenum );
				}
				else
				{
					_trademap[ symbol ] = Trade(symbol, 0, _csvvalues[TIME_COL], _csvvalues[QUANTITY_COL], _csvvalues[PRICE_COL], currweightpricenum);
				}
			
				linecount++;
			}
			else
			{
				chunkend = true;
				break;
			}
		}
		if (( ss.eof() || chunkend ) && !line.empty() && line[0] != NULL)
		{
			_prevchunklastdata = line;
		}

		return linecount;
	}

} // namespace end