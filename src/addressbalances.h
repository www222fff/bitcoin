#ifndef ADDRESSBALANCES_H
#define ADDRESSBALANCES_H

#include <string>
#include <cstdint>
#include <db_cxx.h>
#include <ostream>
#include <map>
#include <vector>
#include <string>

class AddressBalanceDB {
private:
    Db db;
    Db sec_db;

public:
    explicit AddressBalanceDB(const std::string& filename);
    ~AddressBalanceDB();

    // 写入或更新地址余额
    bool WriteBalance(const std::string& addr, uint64_t balance, bool is_add);

    // 遍历所有地址和余额
    std::string GetTotalBalances();
    std::map<std::string, uint64_t> GetBalances(size_t offset, size_t limit);

    bool Clear();
};

extern AddressBalanceDB g_addr_db;
extern AddressBalanceDB g_utxo_db;

#endif // ADDRESSBALANCES_H

