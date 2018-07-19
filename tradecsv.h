#pragma once

#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <numeric>
#include <string>
#include <windows.h>

namespace tradecsv {

	constexpr short TIME_COL = 0;
	constexpr short QUANTITY_COL = 1;
	constexpr short PRICE_COL = 2;

	class Trade
	{
	private:
		uint64_t		_maxtimegap;
		std::string		_symbol;
		uint64_t		_prevtime;
		uint64_t		_totquantity;
		uint64_t		_maxprice;
		uint64_t		_weightpricenum;

	public:
		Trade() {}
		Trade(const std::string& symbol_, const uint64_t maxtimegap_, const uint64_t prevtime_, const uint64_t totquantity_, const uint64_t maxprice_, const uint64_t weightpricenum_)
			:_symbol(symbol_),
			 _maxtimegap(maxtimegap_),
			 _prevtime(prevtime_),
			 _totquantity(totquantity_),
			 _maxprice(maxprice_),
			 _weightpricenum(weightpricenum_)
		{
		}

		const std::string&  symbol() const { return _symbol; }
		const uint64_t		maxtimegap() const { return _maxtimegap; }
		const uint64_t		prevtime() const { return _prevtime; }
		const uint64_t		weightpricenum() const { return _weightpricenum; }
		const uint64_t	    volume() const { return _totquantity; }
		const uint64_t	    maxprice() const { return _maxprice; }
		const uint64_t		weight()   const { return _weightpricenum / _totquantity; }


		void setmaxtimegap(const uint64_t& maxtimegap_) { _maxtimegap = maxtimegap_;  }
		void setvolume(const uint64_t& volume_) { _totquantity = volume_; }
		void setprevtime(const uint64_t& prevtime_) { _prevtime = prevtime_; }
		void setmaxprice(const uint64_t& maxprice_) { _maxprice = maxprice_; }
		void setweightpricenum(const uint64_t& weightpricenum_) { _weightpricenum = weightpricenum_; }

		friend std::ofstream& operator<<(std::ofstream& os_, const std::pair<std::string, Trade>& trade_);
	};



	class CsvParser
	{
	private:
		using TradeMap = std::map<std::string, Trade>;
		TradeMap _trademap;

		std::string _filename;
		std::string _outputfile;
		std::string	_prevchunklastdata{};
		std::vector<char> _fileBuffer{};
		size_t		_totalchunks{ 0 };
		size_t		_chunksize{ 0 };
		size_t		_lastchunksize{ 0 };
		size_t		_numberoffilechunks;
		unsigned    long long _numbytessystem;
		std::ifstream	_fp;
		
		bool		getFileSize(size_t& filesize_);
		bool		determineChunkSizeforfile();
		void		AllocateBuffer();
		uint32_t	parseChunk();
		

		enum DataType { TIME = 0, SYMBOL = 1, QUANTITY = 2, PRICE = 3 };

	public:
		CsvParser(const std::string& fullPath_, size_t chunkfilesize_ = 1);
		~CsvParser();
		CsvParser() = default;

		// trim from left
		inline std::string& ltrim(std::string& s_, const char* t_ = " \t\n\r\f\v\",{}")
		{
			s_.erase(0, s_.find_first_not_of(t_));
			return s_;
		}

		// trim from right
		inline std::string& rtrim(std::string& s_, const char* t_ = " \t\n\r\f\v\",{}")
		{
			s_.erase(s_.find_last_not_of(t_) + 1);
			return s_;
		}


		// trim from left & right
		inline std::string& trim(std::string& s_, const char* t_ = " \t\n\r\f\v\",{}")
		{
			return ltrim(rtrim(s_, t_), t_);
		}

		void setSystemMemoryBytes(unsigned long long numbytes_)
		{
				_numbytessystem = numbytes_;
		}

		unsigned long long getSystemMemoryBytes() const
		{
			return _numbytessystem;
		}

		size_t getNumfileChunks() const
		{
			return _numberoffilechunks;
		}

		const std::string getfilenamebase() const
		{
			//Append the number just before the file extension
			auto pos = _filename.find_last_of(".");
			std::string nameWithoutExt = _filename.substr(0, pos);
			std::string extension = _filename.substr(pos);

			return nameWithoutExt;
		}

		const std::string getfilenameext() const
		{
			//Append the number just before the file extension
			auto pos = _filename.find_last_of(".");
			std::string extension = _filename.substr(pos);

			return extension;
		}

		unsigned long long getTotalSystemMemory()
		{
			MEMORYSTATUSEX status;
			status.dwLength = sizeof(status);
			GlobalMemoryStatusEx(&status);
			return status.ullTotalPhys;
		}

		uint32_t readFile();
		void	 writeOutputFile();
	};

}