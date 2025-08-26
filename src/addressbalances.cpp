#include "addressbalances.h"
#include <cstring>
#include <iostream>
#include <iomanip> 
#include <sstream>

AddressBalanceDB g_addr_db("address_balances");
AddressBalanceDB g_utxo_db("latest_utxo");

AddressBalanceDB::AddressBalanceDB(const std::string& filename)
    : db(nullptr, 0), sec_db(nullptr, 0)
{
    try {
        std::string dbfile = filename + ".db";
        db.open(nullptr, dbfile.c_str(), nullptr, DB_BTREE, DB_CREATE, 0);

        std::string secfile = filename + "_sec.db";
        sec_db.open(nullptr, secfile.c_str(), nullptr, DB_BTREE, DB_CREATE, 0);

    } catch (DbException& e) {
        std::cerr << "Error opening BDB: " << e.what() << std::endl;
        throw;
    }

    //just for testing
    std::string addr1 = "aaa";
    std::string addr2 = "bbb";
    std::string addr3 = "ccc";
    WriteBalance(addr1, 2000000000000000, true);
    WriteBalance(addr2, 2010000000000000, true);
    WriteBalance(addr3, 2020000000000000, true);
    WriteBalance(addr1, 2000000000000000, false);
    WriteBalance(addr2, 2010000000000000, false);
    WriteBalance(addr3, 2020000000000000, false);
}


AddressBalanceDB::~AddressBalanceDB() {
    try {
        sec_db.close(0);
        db.close(0);
    } catch (DbException& e) {
        std::cerr << "Error closing BDB: " << e.what() << std::endl;
    }
}

bool AddressBalanceDB::Clear() {
    try {
        std::lock_guard<std::mutex> lock(db_mutex);
        db.truncate(NULL, NULL, 0); // 清空主数据库，无需计数
        sec_db.truncate(NULL, NULL, 0); // 清空辅助数据库，无需计数
        return true;
    } catch (DbException& e) {
        std::cerr << "Clear error: " << e.what() << std::endl;
        return false;
    } catch (const std::system_error& e) {
        std::cerr << "Lock acquisition failed: " << e.what() << " (code: " << e.code() << ")" << std::endl;
        return false;
    }
}

bool AddressBalanceDB::WriteHeight(int height)
{
    int ret = 0;
    const std::string addr = "blockHeight";
    Dbt key((void*)addr.data(), addr.size());
    Dbt newVal(&height, sizeof(height));

    ret = sec_db.put(nullptr, &key, &newVal, 0);
    if (ret != 0)
    {
        std::cout << "add addr failed " << addr << " ret " << ret << std::endl;
        return false;
    }

    return true;
}

int AddressBalanceDB::GetHeight()
{
    int height = 0;
    const std::string addr = "blockHeight";
    Dbt key((void*)addr.data(), addr.size());
    Dbt value;
    value.set_flags(DB_DBT_MALLOC);
    if (sec_db.get(nullptr, &key, &value, 0) == 0) {
        height = *(int*)value.get_data();
        free(value.get_data());
    }

    return height;
}


bool AddressBalanceDB::WriteBalance(const std::string& addr, uint64_t balance, bool is_add)
{
    uint64_t oldBalance = 0;
    uint64_t newBalance = 0;
    int ret = 0;
    std::stringstream ss;
    Dbt key((void*)addr.data(), addr.size());
    Dbt value;
    value.set_flags(DB_DBT_MALLOC);
    if (db.get(nullptr, &key, &value, 0) == 0) {
        oldBalance = *(uint64_t*)value.get_data();
        free(value.get_data());
    }

    if (is_add)
    {
        newBalance = oldBalance + balance;
    }
    else
    {
        if (oldBalance == 0 || oldBalance < balance)
        {
            return false;
        }
        else
        {
            newBalance = oldBalance - balance;
        }
    }

    if (newBalance == 0 && oldBalance != 0)
    {
        ret = db.del(nullptr, &key, 0);
        if (ret != 0)
            std::cout << "delete addr failed" << addr << " ret " << ret << std::endl;
        
        ss.str("");
        ss << std::hex << std::setfill('0') << std::setw(16) << UINT64_MAX - oldBalance;
        std::string oldSecKey = ss.str() + "_" + addr;
	Dbt oldSKey((void*)oldSecKey.data(), oldSecKey.size());
        
        ret = sec_db.del(nullptr, &oldSKey, 0);
        if (ret != 0)
            std::cout << "delete sec db failed " << oldSecKey << " ret " << ret << std::endl;
    }
    else
    {
        Dbt newVal(&newBalance, sizeof(newBalance));
        ret = db.put(nullptr, &key, &newVal, 0);
        if (ret != 0)
            std::cout << "add addr failed " << addr << " ret " << ret << std::endl;

        if (oldBalance != 0)
        {
            //for sec db need remove old then add new!
            ss.str("");
            ss << std::hex << std::setfill('0') << std::setw(16) << UINT64_MAX - oldBalance;
            std::string oldSecKey = ss.str() + "_" + addr;

            Dbt oldSKey((void*)oldSecKey.data(), oldSecKey.size());
            ret = sec_db.del(nullptr, &oldSKey, 0);
            if (ret != 0)
                std::cout << "del sec db failed " << oldSecKey << " ret " << ret << std::endl;
        }

        if (newBalance != 0)
        {
            ss.str("");
            ss << std::hex << std::setfill('0') << std::setw(16) << UINT64_MAX - newBalance;
            std::string newSecKey = ss.str() + "_" +  addr;
	
            char dummy = 0; 
	    Dbt newSKey((void*)newSecKey.data(), newSecKey.size());
	    Dbt newSVal((void*)&dummy, 1);
	    ret = sec_db.put(nullptr, &newSKey, &newSVal, 0);
            if (ret != 0)
                std::cout << "add sec db failed " << newSecKey << " ret " << ret << std::endl;
        }
    }

    return true;
}

std::string AddressBalanceDB::GetTotalBalances()
{
    int result = 0;
    DB_BTREE_STAT* stats = nullptr;
    if (db.stat(nullptr, &stats, 0) == 0) {
        result = stats->bt_nkeys;
        free(stats);
    }

    int result_sec = 0;
    DB_BTREE_STAT* stats_sec = nullptr;
    if (sec_db.stat(nullptr, &stats_sec, 0) == 0) {
        result_sec = stats_sec->bt_nkeys;
        free(stats_sec);
    }

    return std::to_string(result) + "-" + std::to_string(result_sec);
}

std::map<std::string, uint64_t> AddressBalanceDB::GetBalances(size_t offset, size_t limit)
{
    std::map<std::string, uint64_t> result;
    Dbc* cursor;

    try
    {
	std::lock_guard<std::mutex> lock(db_mutex);
	int ret = sec_db.cursor(nullptr, &cursor, 0);
	if (ret != 0) {
	    throw std::runtime_error("Failed to open cursor on AddressBalanceDB");
	}

	Dbt key, value;
	key.set_flags(DB_DBT_MALLOC);
        value.set_flags(DB_DBT_MALLOC);

        size_t idx = 0;
        while (cursor->get(&key, &value, DB_NEXT) == 0) {
            if (idx >= offset && result.size() < limit) {
                std::string key_str(static_cast<char*>(key.get_data()), key.get_size());
                std::string balance_str = key_str.substr(0, 16);
                uint64_t balance = std::stoull(balance_str, nullptr, 16); 
                //Important Note: Must use sorted key_str as UniValue will resort, if keep addr then output will not be sorted by balance 
                result[key_str] = UINT64_MAX - balance;
            }

            if (key.get_data()) free(key.get_data());
            if (value.get_data()) free(value.get_data());
            ++idx;
            if (result.size() >= limit) break; 
        }

        cursor->close();
        return result;
    } catch (DbException& e) {
        std::cerr << "Clear error: " << e.what() << std::endl;
        return result;
    } catch (const std::system_error& e) {
        std::cerr << "Lock acquisition failed: " << e.what() << " (code: " << e.code() << ")" << std::endl;
        return result;
    }
}

