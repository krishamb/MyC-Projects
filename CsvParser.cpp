

#include "CsvParser.h"
#include <assert.h>

namespace csv {

	using namespace std;

	CsvParser::CsvParser(const std::string& fullPath) : _rows(0), _cols(0), _emptyRows(0), _fileLength(0), _fullPath(fullPath) 
	{	
	}

	CsvParser::~CsvParser() { ; }

	/*** given a file buffer, retrieve number of rows and columns
	* @param fileBuffer pointer to the buffer containing the file
	* @param length of buffer i.e. file length
	* @param maxRows returned value after counting EOL
	* @param maxCols returned value after counting delimiters
	* @return void */

	void CsvParser::determineRowsAndCols(const char* fileBuffer, const std::streamoff length)
	{
		size_t rows = 0; size_t cols = 0; // determine rows and columns

		for (const char* p = fileBuffer; p < fileBuffer + length; ++p)
		{
			switch (*p)
			{
				case '\n': // Linux/Unix 
//				case '\r': // windows uses both CR+LF but here we assume none-windows file. 
					if (*(p + 1) != '\n' && *(p + 1) != '\r')
					if (*(p + 1) != '\n')
						++rows; // check for missing ending delimiter 
					if (p > fileBuffer)
					{
						if (*(p - 1) != ',')
							++cols;
						break;
					}
					break;
				case ',':	++cols;
					break;

			}
		}

		_rows = rows;
		_cols = cols / rows;
	}


	/* this function reads the CSV-file into a buffer that is allocated , this function first checks the size of the file, then allocates a buffer which it puts the file contents
	@param none
	@result true if something was read. */

	bool CsvParser::readFile()
	{
		ifstream fp(_fullPath.c_str(), ios::in | ios::binary);

		if (!fp.is_open())
		{
			return false;
		}
		// get length of file and allocate a buffer of that size

		std::streampos begin = fp.tellg();

		fp.seekg(0, ios::end);
		streampos end = fp.tellg();

		_fileLength = end - begin;

		fp.seekg(0, ios::beg);

		_fileBuffer.resize(static_cast<unsigned int>(_fileLength + 1));
		_outputBuffer.resize(static_cast<unsigned int>(3 * _fileLength + 1));

		fp.read(_fileBuffer.data(), _fileLength);
		fp.close();

		return _fileLength > 0;
	}

	/* returns number of rows in the CSV-file
	@param none
	@result rows in CSV-file */

	size_t CsvParser::rows() const { return _rows; }

	/* returns number of columns in the CSV-file it is assumed the CSV-file follows the standard format
	@param none
	@result cols in CSV-file */

	size_t CsvParser::cols() const { return _cols; }

	/* number of empty rows that have been filtered out normally a CSV-file shouldn't contain empty rows
	@param none
	@result returns the number of empty rows in the file. */

	size_t CsvParser::emptyRows() const { return _emptyRows; }


	/****parses the file and creates a representation of the CSV-file in memory
	the file is loaded into memory and then all delimiters are replaced with \0 valid delimiters are , (comma)
	@param fullPath path to the CSV-file
	@result returns a map representaion of the CSV-file
	use [cols*row + col] to retrieve a token if the file wasn't found, an empty map is returned
	****************/

	bool CsvParser::parseCSV()
	{
		// if called twice, just return previous result. 
		if (_csvFile.size() > 0)
			return true;

		// if failed to read file or empty file, return empty map 
		if (!readFile())
		{
			std::cerr << "file: " << _fullPath.c_str() << "Not found" << std::endl;
			return false;
		}

		// check number of rows and columns 
		determineRowsAndCols(_fileBuffer.data(), _fileLength);

		// initialize pointer to first entry in the CSV-file 
		char* pPreviousToken = _fileBuffer.data();

		location loc;

		_valarrayrow.resize(_rows);
		for (size_t ii = 0; ii < _rows; ++ii)
			_valarrayrow[ii].resize(_cols + 1);

		_valarraycol.resize(_cols + 1);
		for (size_t ii = 0; ii < _cols + 1; ++ii)
			_valarraycol[ii].resize(_rows);

		_csvFile.resize(_rows * _cols + 1);

		// go through the filebuffer putting pointers into the right places.

		for (char* p = _fileBuffer.data(); p < _fileBuffer.data() + _fileLength && loc.col() < _cols && loc.row() < _rows; ++p)
		{
			switch (*p)
			{
			case ',':
				*p = '\0';
				_csvFile[loc.row() * _cols + loc.col()] = pPreviousToken;
				writetovalueArray(loc.row(), loc.col());

				loc.newCol();
				pPreviousToken = p + 1; // start of next token

										// if reached EOL then skip EOL, increment row and reset col.
				if (*pPreviousToken == '\n' || *pPreviousToken == '\r')
				{
					*pPreviousToken++ = '\0';
					++p;
					performOperation(loc);
					loc.newRow();
				}

				break;
			default:
				// ignore empty rows
				if ((*pPreviousToken == '\n' || *pPreviousToken == '\r') && loc.col() < _cols)
				{
					*pPreviousToken++ = '\0';
					loc.resetCol();
					++_emptyRows;
				}
				// end of line but no ending ','
				else if (*p == '\n' )
				{
					*(p - 1) = '\0'; // to handle \r in windows
					*p = '\0';
					_csvFile[loc.row() * _cols + loc.col()] = pPreviousToken;
					writetovalueArray(loc.row(), loc.col());

					pPreviousToken = p + 1;
					performOperation(loc);
					loc.newRow();
				}
				break;
			}
		}

		return true;
	}

	void CsvParser::performOperation(const location& loc)
	{
		if (loc.col() > 1)
		{
			char *op1 = _csvFile[loc.row() * _cols + loc.col() - 2];
			char *op2 = _csvFile[loc.row() * _cols + loc.col() - 1];
			char *op3 = _csvFile[loc.row() * _cols + loc.col()];

			double op1d = atof(op1);
			double op2d = atof(op2);

			double op4;

			switch (*op3)
			{
				case '+': op4 = op1d + op2d;
						  break;
				case '*': op4 = op1d * op2d;
						  break;
				case '/': if (op2d != 0)
						  {
								op4 = op1d / op2d;
						  }
						  else
						  {
								  op4 = 0;
						  }
						  break;
				case '-': op4 = op1d - op2d;
						  break;
			}

			_valarrayrow[loc.row()][loc.col()] = op4;
			_valarraycol[_cols][loc.row()] = op4;
		}

	}

	void CsvParser::quicksort(vector<double>& vec, size_t L, size_t R)
	{
		size_t i, j, mid;
		double piv;

		i = L;
		j = R;
		mid = L + (R - L) / 2;
		piv = vec[mid];

		while (i < R || j > L)
		{
			while (vec[i] < piv)
				i++;
			while (vec[j] > piv)
				j--;

			if (i <= j)
			{
				std::swap(vec[i], vec[j]);
				i++;
				j--;
			}
			else
			{
				if (i < R)
					quicksort(vec, i, R);
				if (j > L)
					quicksort(vec, L, j);
				return;
			}
		}
	}

	// write a given row into output buffer

	void CsvParser::writeRowtoOutputBuffer(const size_t row)
	{
		for (size_t col = 0; col < _cols; ++col)
		{
			writetoOutputBuffer(row, col);
		}

		writeLastColtoOutputBuffer(row);

		_outputBuffer.push_back('\n');
	}

	void CsvParser::writeLastColtoOutputBuffer(const size_t row_)
	{
		std::string str(to_string(_valarraycol[_cols][row_]));

		_outputBuffer.push_back(',');		
		_outputBuffer.insert(_outputBuffer.end(), str.begin(), str.end());
		_outputBuffer.push_back('\n');
	}

	// writes the value in (row,col ) into output buffer

	void CsvParser::writetoOutputBuffer(const size_t row_, const size_t col_)
	{
		if (col_ != 0)
			_outputBuffer.push_back(',');

		char *ptr = _csvFile[row_ * _cols + col_];

		if ( ptr != nullptr )
		{
			_outputBuffer.insert(_outputBuffer.end(), ptr, ptr + strlen(ptr));
		}
		else
		{
			const char *cptr = " ";
			_outputBuffer.insert(_outputBuffer.end(), cptr, cptr + strlen(cptr));
		}
	}

	// writes the given row, col value after converting to double into value row and column array

	void CsvParser::writetovalueArray(const size_t row_, const size_t col_)
	{
		double val = 0.0;

		if (_csvFile[row_ * _cols + col_] != nullptr)
		{
			char *ptr = _csvFile[row_ * _cols + col_];

			if (isdigit(*ptr))
			{
				val = atof(ptr);
			}
			else
			{
				val = 0.0;
			}
		}
		else
		{
			val = 0.0;
		}

		_valarrayrow[row_][col_] = val;
		_valarraycol[col_][row_] = val;
#ifdef _DEBUG
		std::cout << "_valarraycol[" << col_ << "]" << "[" << row_ << "]" << _valarraycol[col_][row_] << std::endl;
		std::cout << "_valarrayrow[" << row_ << "]" << "[" << col_ << "]" << _valarrayrow[row_][col_] << std::endl;
#endif
	}

	void CsvParser::writetofile()
	{
		string str;
		for (size_t ii = 0; ii < _cols + 1; ii++)
			str += "C" + to_string(ii + 1) + ",";

		writeStatString(str);
		_outputBuffer.push_back('\n');

		for (size_t row = 0; row < _rows; ++row)
		{
			for (size_t col = 0; col < _cols; ++col)
			{
				writetoOutputBuffer(row, col);
			}

			writeLastColtoOutputBuffer(row);
		}

		_outputBuffer.push_back('\n');
		_outputBuffer.push_back('\n');
		_outputBuffer.push_back('\n');

		for (size_t c = 0; c < _cols + 1; ++c)
		{
			quicksort(_valarraycol[c], 0, _valarraycol[c].size() - 1);

			double average = std::accumulate(_valarraycol[c].begin(), _valarraycol[c].end(), 0.0) / _valarraycol[c].size();

			double median = calcMedian(_valarraycol[c]);

			size_t size = _valarraycol[c].size();
			if (c != OPCOL)
			{
				_stats[Min] += std::to_string(_valarraycol[c][0]) + ",";
				_stats[Max] += std::to_string(_valarraycol[c][size - 1]) + ",";
				_stats[Avg] += std::to_string(average) + ",";
				_stats[Median] += std::to_string(median) + ",";
			}
			else
			{
				_stats[Min] += " ,";
				_stats[Max] += " ,";
				_stats[Avg] += " ,";
				_stats[Median] += " ,";
			}
		}

		for (size_t ii = 0; ii < _stats.size(); ii++)
		{
			string& str = _stats[ii];

			str.replace(str.length() - 1, 1, 1, '\n');

			writeStatString(str);
			_outputBuffer.push_back('\n');
			_outputBuffer.push_back('\n');
		}

		writeOutputBufferToFile();
	}

	void CsvParser::writeOutputBufferToFile()
	{
		std::string outputfile = "./output.csv";
		std::ofstream fp(outputfile, ios::out | ios::binary| ios::trunc);
		fp.write(_outputBuffer.data(), _outputBuffer.size());
		fp.close();
	}

	void CsvParser::writeStatString(const string& str_)
	{
		_outputBuffer.insert(_outputBuffer.end(), str_.begin(), str_.end());
	}

	double CsvParser::calcMedian(vector<double>& columns)
	{
		size_t size = columns.size();

		if (size == 0)
		{
			return 0;  // Undefined, really.
		}
		else if (size == 1)
		{
			return columns[0];
		}
		else
		{
			if (size % 2 == 0)
			{
				return (columns[size / 2 - 1] + columns[size / 2]) / 2;
			}
			else
			{
				return columns[size / 2];
			}
		}
	}

	/**** Assumptions:- Same table join between two columns, returning matching rows by writing to a file ****/
	/***** innerjoin will return the rows that are matched between col1 and col2 same table *****
	****** col parameters denotes the actual column number like 0,1,2 and so on *********/
	/*** It can be easily expanded to include other two tables each taking CSVParser object pointing its data set****/
	void CsvParser::innerjoin(size_t col1_, size_t col2_)
	{
		// if parseCSV was not called previously, do it now, ignore return value 
		if (_rows == 0 && _cols == 0)
		{
			bool ret = parseCSV(); // if fail, just return

			if (!ret)
				return;
		}

		assert(col1_ < _cols);
		assert(col2_ < _cols);

		for (size_t row = 0; row < _rows; ++row)
		{
			if (IsEqual(_valarrayrow[row][col1_], _valarrayrow[row][col2_]))
			{
				writeRowtoOutputBuffer(row);
			}
		}

		writeOutputBufferToFile();
	}

	/**** Assumptions:- Same table join between two columns, returning matching rows by writing to a file ****/
	/***** innerjoin will return the rows that are matched between col1 and col2 same table *****
	****** col parameters denotes the actual column number like 0,1,2 and so on *********/
	/*** It can be easily expanded to include other two tables each taking CSVParser object pointing its data set****/
	void CsvParser::outerjoin(size_t col1_, size_t col2_, bool leftjoin)
	{
		// if parseCSV was not called previously, do it now, ignore return value 
		if (_rows == 0 && _cols == 0)
		{
			bool ret = parseCSV(); // if fail, return NULL

			if (!ret)
				return;
		}

		assert(col1_ < _cols);
		assert(col2_ < _cols);

		for (size_t row = 0; row < _rows; ++row)
		{
			if (IsEqual(_valarrayrow[row][col1_], _valarrayrow[row][col2_]))
			{
				writeRowtoOutputBuffer(row);
			}
			else
			{
				for (size_t col = 0; col < _cols; ++col)
				{
					if (leftjoin)
					{
						if (col != col2_)
						{
							writetoOutputBuffer(row, col);
						}
						else
						{
							if (col != 0)
								_outputBuffer.push_back(',');

							const char *cptr = "*";
							_outputBuffer.insert(_outputBuffer.end(), cptr, cptr + strlen(cptr));
						}
					}
					else
					{
						if (col != col1_)
						{
							writetoOutputBuffer(row, col);
						}
						else
						{
							if (col != 0)
								_outputBuffer.push_back(',');

							const char *cptr = "*";
							_outputBuffer.insert(_outputBuffer.end(), cptr, cptr + strlen(cptr));
						}
					}
				}
			}

			if (_cols + 1 != col1_ || _cols + 1 != col2_)
			{
				writeLastColtoOutputBuffer(row);
			}
			else
			{
				const char *cptr = "*";
				_outputBuffer.insert(_outputBuffer.end(), cptr, cptr + strlen(cptr));

			}

			_outputBuffer.push_back('\n');
		}

		writeOutputBufferToFile();
	}



	/* can be used to see how to access contents of the CSV-file and dump the file to standard output after calling parser routine
	@param none
	@result void */

	void CsvParser::dumpfile()
	{
		// print out the file contents read
		for (size_t r = 0; r < _rows; ++r)
		{
			for (size_t c = 0; c < _cols; ++c)
			{
				if (c != 0)
					std::cout << '\t';

				if (_csvFile[r * _cols + c] != NULL)
				{
					std::cout << _csvFile[r*_cols + c];
				}
				else
				{
					std::cout << "*";
				}
			}

			std::cout << endl;
		}

		std::cout << "empty rows: " << _emptyRows << endl;
	}

	/*****
	alternative way to retrieve one value without the map
	a slower way to retrieve a token, but maybe safer
	@param row row starting at 0 to rows()-1
	@param col column starting at 0 to cols()-1
	@result one string token ******/

	char* CsvParser::token(size_t row_, size_t col_)
	{
		// if parseCSV was not called previously, do it now, ignore return value 
		if (_rows == 0 && _cols == 0)
		{
			if (!parseCSV())
				return nullptr;// if fail, return NULL
		}

		assert(row_ < _rows);
		assert(col_ < _cols);

		// avoid memory out of bonds 
		if (row_ >= _rows || col_ >= _cols)
		{
			return nullptr;
		}

		return _csvFile[row_ * _cols + col_];
	}
}