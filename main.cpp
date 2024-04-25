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
        Node() {
            val = Null();
        }
        Node(Value x) {
            this->val = x;
        }
    };

    std::ostream& operator<<(std::ostream &cout,const Value &x){
        if(std::holds_alternative<Object>(x)) {
            cout << "{";
            int flag = 0;
            for(auto [p,q] : std::get<Object>(x)) {
                if(flag)cout << ',';
                cout << '\"' << p << '\"' << ':' << q.val;
                flag = 1;
            }
            cout << "}";
        }else if(std::holds_alternative<Array>(x)) {
            cout << "[";
            int flag = 0;
            for(auto & p : std::get<Array>(x)) {
                if(flag)cout << ',';
                cout << p.val;
                flag = 1;
            }
            cout << "]";
        }else if(std::holds_alternative<String>(x)) {
            cout << "\"" << std::get<String>(x) << "\"";
        }else if(std::holds_alternative<Bool>(x)) {
            bool flag = std::get<Bool>(x);
            if(flag)cout << "true";
            else cout << "false";
        }else if(std::holds_alternative<Int>(x)) {
            cout << std::get<Int>(x);
        }else if(std::holds_alternative<Float>(x)) {
            cout << std::get<Float>(x);
        }else if(std::holds_alternative<Null>(x)) {
            cout << "null";
        }
        return cout;
    }
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
            while(pos < len && json_str[pos] != ']') {
                skip_whitespace();
                res.push_back(judge_kind().value());
                skip_whitespace();
                if(json_str[pos] == ',')pos++;
            }
            pos++;
            return Node(Value(res));
        }
        std::optional<Node> parser_obj() {
            Object res;
            pos++;
            while(pos < len && json_str[pos] != '}') {
                skip_whitespace();
                String key = std::get<std::string>(parser_str().value().val);
                skip_whitespace();
                assert(json_str[pos] == ':');
                pos++;
                skip_whitespace();
                Node val__ = judge_kind().value();
                res[key] = val__;
                skip_whitespace();
                if(json_str[pos] == ',')pos++;
            }
            pos++;
            return Node(Value(res));
        }
        std::optional<Node> parser_str() {
            String res;
            pos++;
            while(pos < len && json_str[pos] != '\"') {
                res += json_str[pos];
                pos++;
            }
            pos++;
            return Node(Value(res));
        }
        std::optional<Node> parser_num() {
            Int res = 0;
            bool flag = false;
            long long div = 1;
            while(pos < len && (isdigit(json_str[pos]) || json_str[pos] == '.')) {
                if(json_str[pos] == '.') {
                    flag = true;
                }else{
                    res = res * 10 + (json_str[pos] - '0');
                    if(flag)div *= 10;
                }
                pos++;
            }
            res = res / div;
            return Node(Value(res));
        }
        std::optional<Node> parser_null() {
            assert(pos < len && json_str[pos] == 'n');
            pos++;
            assert(pos < len && json_str[pos] == 'u');
            pos++;
            assert(pos < len && json_str[pos] == 'l');
            pos++;
            assert(pos < len && json_str[pos] == 'l');
            pos++;
            Null res;
            return Node(Value(res));
        }
        std::optional<Node> parser_bool() {
            std::string bool_s;
            if(json_str[pos] == 't') {
                for(int i = 0; i < 4; ++i) {
                    assert(pos < len);
                    bool_s += json_str[pos];
                    pos++;
                }
            }else if(json_str[pos] == 'f'){
                for(int i = 0; i < 5; ++i) {
                    assert(pos < len);
                    bool_s += json_str[pos];
                    pos++;
                }
            }
            assert(bool_s == "true" || bool_s == "false");
            if(bool_s == "true")return Node(Value(true));
            else return Node(Value(false));
        }
        std::optional<Node> judge_kind() {
            // static int cnt = 0;
            // std::cout << "judge_kind" << ++cnt << '\n';
            skip_whitespace();
            if (json_str[pos] == '[')
                return parser_arr();
            if(json_str[pos] == '{')
                return parser_obj();
            if(json_str[pos] == '\"')
                return parser_str();
            if (isdigit(json_str[pos]))
                return parser_num();
            if (json_str[pos] == 'n')
                return parser_null();
            if (json_str[pos] == 't' || json_str[pos] == 'f')
                return parser_bool();
            return {};
        }
    public:
        json_parser(const std::string &str) {
            json_str = str;
            pos = 0;
            len = str.size();
        }
        std::optional<Node> solve() {
            auto res = judge_kind().value().val;
            if(std::holds_alternative<Object>(res) || std::holds_alternative<Array>(res))
                return res;
            return {};
        }
        void show() {
            for(int i = 0; i < len; ++i) {
                std::cout << (int)json_str[i] << '\n';
            }
        }
    };
}
using namespace json;
bool update(Value& x,std::string& key,Node& new_val) {
    int flag = false;
    if(std::holds_alternative<Object>(x)) {
        for(auto &[p,q] : std::get<Object>(x)) {
            if(p == key) {
                q = new_val;
                flag = true;
            }
            if(update(q.val,key,new_val))
                flag = true;
        }
    }else if(std::holds_alternative<Array>(x)) {
        for(auto & p : std::get<Array>(x)) {
            if(update(p.val,key,new_val))
                flag = true;
        }
    }
    return flag;
}
signed main() {
    std::ifstream fin("../json.txt");
    std::stringstream ss; ss << fin.rdbuf();
    std::string s{ ss.str() };
    json_parser p(s);
    auto res = p.solve().value();
    std::cout << res.val;
    std::string key = "friends";
    Node new_val = Node(Null());
    std::cout << '\n' << update(res.val,key,new_val) << '\n';
    std::cout << res.val;
    return 0;
}