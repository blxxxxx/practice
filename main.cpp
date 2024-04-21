#include<bits/stdc++.h>
namespace json {
    struct Node;

    using Null = std::monostate;
    using Int = long long;
    using Float = double;
    using Bool = bool;
    using String = std::string;
    using Object = std::map<String,Node>;
    using Array = std::vector<Node>;
    using Value = std::variant<Null,Int,Float,Bool,String,Object,Array>;

    using Readstr = std::string_view;
    struct Node {
        Value val;
        Node(Value x) {
            val = x;
        }
    };
    class json_parser {
        Readstr json_str;
        std::size_t pos,len;
        void skip_whitespace() {
            while(pos < len && (json_str[pos] == ' ' || json_str[pos] == '\n')) {
                pos++;
            }
        }
        std::optional<Node> parser_arr() {
            Array res;
            pos++;
            skip_whitespace();
            while(json_str[pos] != ']') {
                res.push_back(judge_kind().value());
                skip_whitespace();
            }
            pos++;
            return Node(Value(res));
        }
        std::optional<Node> parser_obj() {

        }
        std::optional<Node> parser_str() {

        }
        std::optional<Node> parser_num() {

        }
        std::optional<Node> parser_null() {

        }
        std::optional<Node> parser_bool() {

        }
        std::optional<Node> judge_kind() {
            skip_whitespace();
            if constexpr (json_str[pos] == '[')
                return parser_arr();
            else if constexpr (json_str[pos] == '{')
                return parser_obj();
            else if constexpr (json_str[pos] == '\"')
                return parser_str();
            else if constexpr (isdigit(json_str[pos]))
                return parser_num();
            else if constexpr (json_str[pos] == 'n')
                return parser_null();
            else if constexpr (json_str[pos] == 't' || json_str[pos] == 'f')
                return parser_bool();
            else throw std::runtime_error("start of illegal char");
            return {};
        }
    public:
        json_parser(const std::string &str) {
            json_str = str;
            pos = 0;
            len = str.size();
        }

        void show() {
            for(int i = 0; i < len; ++i) {
                std::cout << (int)json_str[i] << '\n';
            }
        }
    };
}


signed main() {
    std::ifstream fin("../json.txt");
    std::stringstream ss; ss << fin.rdbuf();
    std::string s{ ss.str() };
    json::json_parser p(s);
    p.show();
}