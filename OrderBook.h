#pragma once

#include <iostream>
#include <vector>
#include <fstream>
#include <numeric>
#include <string>
#include <queue>
#include <limits>
#include <sstream>
#include <set>
#include <unordered_map>
#include <chrono>
#include <cstdint>
#include <iterator>

namespace OB
{
   /* Price represented as integer, max is 4294967295 
    0- interpreted as divided by 100
    eg the range is 000.00-655.36
    eg the price 123.45 = 12345
    eg the price 23.45 = 2345
    eg the price 23.4 = 2340 */

	enum class QuoteProcessStatus { 
									MISSING_ATTRIBUTES,
									INVALID_QUOTE,
									INVALID_SIDE,
									INVALID_ORDERTYPE,
									INVALID_VOLUME,
									INVALID_PRICE,
									QUOTE_PROCESSED, 
									QUOTEVOLUME_NOT_FOUND, 
									QUOTE_NOT_FOUND,
									QUOTE_FAILURE,
									QUOTE_DELETE_FAILURE,
									QUOTE_SUCCESS
								   };

	using t_price = uint16_t;

	constexpr t_price kMaxPrice = std::numeric_limits<t_price>::max();

	constexpr t_price kMinPrice = 1;

	constexpr t_price PRICEFACTOR = 100;

	constexpr char    DELIM = '/';
	constexpr uint8_t MAXQUOTEATTRIBUTES = 5;
	constexpr uint8_t QUOTE = 0;
	constexpr uint8_t SIDE = 1;
	constexpr uint8_t ORDERTYPE = 2;
	constexpr uint8_t PRICE = 3;
	constexpr uint8_t VOLUME = 4;

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
	inline std::string& trim(std::string & s_, const char* t_ = " \t\n\r\f\v\",{}")
	{
		return ltrim(rtrim(s_, t_), t_);
	}

	void init();
	void destroy();
	void printOrderBook();

	/* Process an incoming limit order */
	QuoteProcessStatus processlimit(const std::string& quoteorder_);

	class CurrentTime 
	{
	private:
		std::chrono::high_resolution_clock m_clock;

	public:
		uint64_t milliseconds()
		{
			return std::chrono::duration_cast<std::chrono::milliseconds>
				(m_clock.now().time_since_epoch()).count();
		}
		uint64_t microseconds()
		{
			return std::chrono::duration_cast<std::chrono::microseconds>
				(m_clock.now().time_since_epoch()).count();
		}
		uint64_t nanoseconds()
		{
			return std::chrono::duration_cast<std::chrono::nanoseconds>
				(m_clock.now().time_since_epoch()).count();
		}
	};

	class Order
	{
	private:
		std::string _quote;
		char		_side;
		char		_ordertype;
		t_price     _price;
		uint32_t    _volume;
		uint64_t    _timestamp;

	public:
		Order()
		{
		}
		Order(const std::string quote_, const char side_, const char ordertype_, const uint32_t volume_, const uint64_t timestamp_)
			: _quote(quote_),
			  _side(side_),
			  _ordertype(ordertype_),
			  _volume(volume_),
			  _timestamp(timestamp_)
		{
		}
		Order(const std::string quote_, uint32_t volume_, const uint64_t timestamp_): _quote(quote_), _volume(volume_), _timestamp(timestamp_)
		{
		}

		friend class OrderCmp;

		const std::string& getquote() const { return _quote;  }
		void setVolume(const uint32_t volume_) { _volume = volume_; }
		const uint32_t getVolume() const { return _volume; }
		const uint64_t gettimestamp() const { return _timestamp; }
		const char getside() const { return _side; }
	};

	class OrderCmp
	{
	public:
		bool operator () (const Order& left_, const Order& right_) const
		{
			if (left_._quote != right_._quote)
			{
				if (left_._volume > right_._volume) 
					return true;
				if (left_._volume == right_._volume && left_._timestamp > right_._timestamp) 
					return true;
			}

			return false;
		}
	};

	using Book = std::set<Order, OrderCmp>;

	class OrderBook;

	class OrderEntry
	{
	public:
		OrderEntry() {}
		Book	 _order;

		Book& getBook() { return _order; }
		const Book& getBook() const { return _order; }
	};
	
	class LastQuote
	{
	private:
		char		_lastside;
		t_price     _lastprice;
		uint32_t    _lastvolume;
		uint64_t    _lasttimestamp;

	public:
		LastQuote(char side_, t_price priceslot_, uint32_t volume_, uint64_t currenttime_) 
			: _lastside(side_), 
			 _lastprice(priceslot_),
			 _lastvolume(volume_), 
			 _lasttimestamp(currenttime_)
		{
		}

		t_price  getLastprice() const { return _lastprice; }
		char     getLastside() const { return _lastside; }
		uint32_t getLastvolume() const { return _lastvolume; }
		uint64_t getLasttimestamp() const { return _lasttimestamp; }

		void setLastprice(t_price price_)
		{
			_lastprice = price_;
		}
		void setLastside(char side_)
		{
			_lastside = side_;
		}
		void setLastvolume(uint32_t volume_)
		{
			_lastvolume = volume_;
		}
		void setLasttimestamp(uint64_t timestamp_)
		{
			_lasttimestamp = timestamp_;
		}
	};

	class OrderBook
	{
	public:
		OrderBook& operator = (const OrderBook&) = delete;
		OrderBook(const OrderBook&) = delete;
		OrderBook(OrderBook&&) = delete;
		OrderBook& operator = (OrderBook&&) = delete;

		static OrderBook& get();

		void initialize();

		void shutdown();

		QuoteProcessStatus limit(const std::string& orderentry_);

		const std::vector <OrderEntry>& getBuyBook() const
		{
			return _buyBook;
		}

		const std::vector <OrderEntry>& getAskBook() const
		{
			return _askBook;
		}

		bool getQuoteAttributes(const std::string& quote_, std::vector<std::string>& tokens)
		{
			std::stringstream ss(quote_);
			std::string word;

			while (std::getline(ss, word, DELIM))
			{
				if (word.empty())
					return false;

				trim(word);
				tokens.push_back(word);
			}

			return tokens.size() == MAXQUOTEATTRIBUTES;
		}

		OrderBook() { }

		const t_price getAskMin() const { return _askMin; }
		const t_price getBidMax() const { return _bidMax; }
		const t_price getAskMax() const { return _askMax; }
		const t_price getBidMin() const { return _bidMin; }

		void printBBO() const
		{
			std::cout << "\nAsk Min: " << getAskMin() << " Best Bid: " << getBidMax();
		}

	    void printOrderBook()
		{
			std::cout << "\nASK\n";

			const std::vector<OrderEntry>& buybook = getBuyBook();
			const std::vector<OrderEntry>& askbook = getAskBook();

			for (t_price price = getAskMin(); price <= getAskMax(); price++)
			{
				auto ppEntry = askbook.begin() + price;

				auto bookentry = ppEntry->getBook();

				auto orderit = bookentry.cbegin();

				while (orderit != bookentry.cend())
				{
					std::cout << orderit->getquote() << "/" << (float )price / PRICEFACTOR << "/" << orderit->getVolume() << "\n";
					++orderit;
				}
			}

			std::cout << "\nBUY\n";

			for (t_price price = getBidMax(); price >= getBidMin(); price--)
			{
				auto ppEntry = buybook.begin() + price;

				auto bookentry = ppEntry->getBook();

				auto orderit = bookentry.cbegin();

				while (orderit != bookentry.cend())
				{
					std::cout << orderit->getquote() << "/" << (float ) price / PRICEFACTOR << "/" << orderit->getVolume() << "\n";
					++orderit;
				}
			}
		}

		template <class Q>
		void clearQueue(Q& q) {
			q = Q();
		}
		
	private:
		OrderEntry& getOrderEntry(char side_, t_price priceslot_)
		{
			if (side_ == 'B')
			{
				return _buyBook[priceslot_];
			}
			else
			{
				return _askBook[priceslot_];
			}
		}

		QuoteProcessStatus InsertQuoteIntoOrderBook(t_price priceslot_, const std::string quoteid_, const char side_, 
													const char ordertype_, const uint32_t volume_)
		{
			uint64_t currenttime = _localtime.nanoseconds();

			Order neworder(quoteid_, side_, ordertype_, volume_, currenttime);
			
			if (side_ == 'B')
			{
				if (priceslot_ > _buyBook.size())
					_buyBook.resize(priceslot_);

				auto ret = _buyBook[ priceslot_ ]._order.emplace( std::move(neworder) );

				if (!ret.second)
				{
#ifdef _DEBUG
					std::cout << "\nQuote insertion failure for " << quoteid_;
#endif
					return QuoteProcessStatus::QUOTE_FAILURE;
				}
			}
			else
			{
				if (priceslot_ > _askBook.size())
					_askBook.resize(priceslot_);

				auto ret = _askBook[ priceslot_ ]._order.emplace( std::move(neworder) );

				if (!ret.second)
				{
#ifdef _DEBUG
					std::cout << "\nQuote insertion failure for " << quoteid_;
#endif
					return QuoteProcessStatus::QUOTE_FAILURE;
				}
			}

			UpdateBestBidAsk(side_, priceslot_);
			UpdateLastQuote(quoteid_, side_,  priceslot_, volume_, currenttime );

			return QuoteProcessStatus::QUOTE_PROCESSED;
		}

		QuoteProcessStatus UpdateOrderBook(const t_price priceslot_, const std::string& quoteid_, const char side_, 
										   const char ordertype_, const uint32_t volume_)
		{
			QuoteProcessStatus ret = deleteQuotefromOrderBook(quoteid_, side_);

			if (ret == QuoteProcessStatus::QUOTE_PROCESSED)
			{
				ret = InsertQuoteIntoOrderBook(priceslot_, quoteid_, side_, ordertype_, volume_);
			}

			return ret;
		}

		// Delete a specific quote from the appropriate order book
		QuoteProcessStatus deleteQuotefromOrderBook(const std::string& quoteid_, const char side_)
		{
			LastQuoteMap* lastquoteptr;

			if (side_ == 'B')
			{
				lastquoteptr = &_lastbuyquote;
			}
			else
			{
				lastquoteptr = &_lastsellquote;
			}

			auto lastit = lastquoteptr->find(quoteid_);

			if (lastit == lastquoteptr->end())
			{
#ifdef _DEBUG
				std::cout << "\nUpdateOrderBook: Quote: " << quoteid_ << " Not found";
#endif
				return QuoteProcessStatus::QUOTE_NOT_FOUND;
			}

			LastQuote& lastquote = lastit->second;

			OrderEntry& orderentry = getOrderEntry(side_, lastquote.getLastprice());
			Book& book = orderentry.getBook();

			auto it = book.find(Order(quoteid_, lastquote.getLastvolume(), lastquote.getLasttimestamp()));

			if (it == book.end())
			{
				std::cerr << "\nUpdateOrderBook: volume " << lastquote.getLastvolume() << "Timestamp " << lastquote.getLasttimestamp() << "Not found in book\n";
				return QuoteProcessStatus::QUOTEVOLUME_NOT_FOUND;
			}

			//price change or volume change or both so delete earlier entry, adjust prev agg volume, update to new price/volume,update new agg volume
			// for new price or old price update

			if (book.size() == 1)
			{
				UpdateBestBidAsk(side_, lastquote.getLastprice());
			}

			book.erase(it);
			lastquoteptr->erase(lastit);

			return QuoteProcessStatus::QUOTE_PROCESSED;
		}

		QuoteProcessStatus deleteOrderBook(const char side_)
		{
			if (side_ == 'B')
			{
				_buyBook.clear();
				_lastbuyquote.clear();

				_buyBook.shrink_to_fit();
				return QuoteProcessStatus::QUOTE_PROCESSED;
			}
			else if (side_ == 'A')
			{
				_askBook.clear();
				_lastsellquote.clear();

				_askBook.shrink_to_fit();
				if (_askBook.size() == 0)
					return QuoteProcessStatus::QUOTE_PROCESSED;
			}

			return QuoteProcessStatus::QUOTE_DELETE_FAILURE;
		}

		QuoteProcessStatus UpdateLastQuote(const std::string& quoteid_, const char side_, const t_price priceslot_, 
							 const uint32_t volume_, const uint64_t currenttime_)
		{
			LastQuoteMap* lastquote;

			if (side_ == 'B')
			{
				lastquote = &_lastbuyquote;
			}
			else
			{
				lastquote = &_lastsellquote;
			}

			auto it = lastquote->find(quoteid_);

			if (it == lastquote->end())
			{
				// new quote
				LastQuote quote(side_, priceslot_, volume_, currenttime_);
				auto result = lastquote->insert(std::make_pair(quoteid_, std::move(quote)));

				if (!result.second)
				{
					std::cerr << quoteid_ << " Not Inserted in LastQuote Map\n";
					return QuoteProcessStatus::QUOTE_FAILURE;
				}
#ifdef _DEBUG
				std::cout << "\nInserted Lastquote " << quoteid_ << " Successfully";
#endif
			}
			else
			{
				it->second.setLastprice(priceslot_);
				it->second.setLastside(side_);
				it->second.setLasttimestamp(currenttime_);
				it->second.setLastvolume(volume_);
			}

			return QuoteProcessStatus::QUOTE_SUCCESS;
		}

		void UpdateBestBidAsk(const char side_, t_price priceslot_)
		{
			if (side_ == 'B')
			{
				if (priceslot_ > getBidMax())
				{
					_prevbidMax = getBidMax();
					_bidMax = priceslot_;
				}

				if (priceslot_ < getBidMin())
				{
					_prevbidMin = getBidMin();
					_bidMin = priceslot_;
				}
			}
			else
			{
				if (priceslot_ < getAskMin())
				{
					_prevaskMin = getAskMin();
					_askMin = priceslot_;
				}

				if (priceslot_ > getAskMax())
				{
					_prevaskMax = getAskMax();
					_askMax = priceslot_;
				}
			}
		}
		
		std::vector<OrderEntry> _buyBook, _askBook; // pricepoints , price is represented in integer

		using LastQuoteMap = std::unordered_map<std::string, LastQuote>;
		LastQuoteMap _lastbuyquote, _lastsellquote;
		// Minimum Ask price
		t_price _askMin, _prevaskMin;
		// Maximum Bid price
		t_price _bidMax, _prevbidMax;
		// Minimum Bid price
		t_price _bidMin, _prevbidMin;
		// Maximum Ask price
		t_price _askMax, _prevaskMax;
		CurrentTime _localtime;
	};
}