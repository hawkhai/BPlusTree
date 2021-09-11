#ifndef ToyIndexGOGO_HH
#define ToyIndexGOGO_HH

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

//#define TEST
#ifdef TEST

#define BP_ORDER 4

typedef int ToyValuek;
struct ToyKeyk {
    char k[16];

    ToyKeyk(const char* str = "")
    {
        memset(k, 0, sizeof(k));
        strcpy(k, str);
    }

    operator bool() const {
        return strcmp(k, "");
    }
};

inline int keycmp(const ToyKeyk& l, const ToyKeyk& r) {
    return strcmp(l.k, r.k);
}

#define OPERATOR_KEYCMP(type) \
    bool operator< (const ToyKeyk &l, const type &r) {\
        return keycmp(l, r.key) < 0;\
    }\
    bool operator< (const type &l, const ToyKeyk &r) {\
        return keycmp(l.key, r) < 0;\
    }\
    bool operator== (const ToyKeyk &l, const type &r) {\
        return keycmp(l, r.key) == 0;\
    }\
    bool operator== (const type &l, const ToyKeyk &r) {\
        return keycmp(l.key, r) == 0;\
    }

#else

#define BP_ORDER 20

typedef int ToyValuek;
struct ToyKeyk {
    char k[16];

    ToyKeyk(const char* str = "")
    {
        memset(k, 0, sizeof(k));
        strcpy(k, str);
    }

    operator bool() const {
        return strcmp(k, "");
    }
};

inline int keycmp(const ToyKeyk& a, const ToyKeyk& b) {
    int x = strlen(a.k) - strlen(b.k);
    return x == 0 ? strcmp(a.k, b.k) : x;
}

#define OPERATOR_KEYCMP(type) \
    bool operator< (const ToyKeyk &l, const type &r) {\
        return keycmp(l, r.key) < 0;\
    }\
    bool operator< (const type &l, const ToyKeyk &r) {\
        return keycmp(l.key, r) < 0;\
    }\
    bool operator== (const ToyKeyk &l, const type &r) {\
        return keycmp(l, r.key) == 0;\
    }\
    bool operator== (const type &l, const ToyKeyk &r) {\
        return keycmp(l.key, r) == 0;\
    }

#endif

#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(ToyMeta)
#define SIZE_NO_CHILDREN sizeof(ToyLeafNode) - BP_ORDER * sizeof(ToyRecord)


typedef struct {
    size_t order; 
    size_t value_size; 
    size_t key_size;   
    size_t internal_node_num; 
    size_t leaf_node_num;     
    size_t height;            
    off_t slot;        
    off_t root_offset; 
    off_t leaf_offset; 
} ToyMeta;


struct ToyIndex {
    ToyKeyk key;
    off_t child; 
};


struct ToyInternalNode {
    typedef ToyIndex* child_t;

    off_t parent; 
    off_t next;
    off_t prev;
    size_t n; 
    ToyIndex children[BP_ORDER];
};


struct ToyRecord {
    ToyKeyk key;
    ToyValuek value;
};


struct ToyLeafNode {
    typedef ToyRecord* child_t;

    off_t parent; 
    off_t next;
    off_t prev;
    size_t n;
    ToyRecord children[BP_ORDER];
};


class ToyIndexGOGO {
public:
    ToyIndexGOGO(const char* path, bool force_empty = false);

    
    int search(const ToyKeyk& key, ToyValuek* value) const;
    int search_range(ToyKeyk* left, const ToyKeyk& right,
        ToyValuek* values, size_t max, bool* next = NULL) const;
    int remove(const ToyKeyk& key);
    int insert(const ToyKeyk& key, ToyValuek value);
    int update(const ToyKeyk& key, ToyValuek value);
    ToyMeta get_meta() const {
        return meta;
    };

#ifdef TEST
public:
#else
private:
#endif
    char path[512];
    ToyMeta meta;

    
    void init_from_empty();

    
    off_t search_index(const ToyKeyk& key) const;

    
    off_t search_leaf(off_t index, const ToyKeyk& key) const;
    off_t search_leaf(const ToyKeyk& key) const
    {
        return search_leaf(search_index(key), key);
    }

    
    void remove_from_index(off_t offset, ToyInternalNode& node,
        const ToyKeyk& key);

    
    bool borrow_key(bool from_right, ToyInternalNode& borrower,
        off_t offset);

    
    bool borrow_key(bool from_right, ToyLeafNode& borrower);

    
    void change_parent_child(off_t parent, const ToyKeyk& o, const ToyKeyk& n);

    
    void merge_leafs(ToyLeafNode* left, ToyLeafNode* right);

    void merge_keys(ToyIndex* where, ToyInternalNode& left,
        ToyInternalNode& right, bool change_where_key = false);

    
    void insert_record_no_split(ToyLeafNode* leaf,
        const ToyKeyk& key, const ToyValuek& value);

    
    void insert_key_to_index(off_t offset, const ToyKeyk& key,
        off_t value, off_t after);
    void insert_key_to_index_no_split(ToyInternalNode& node, const ToyKeyk& key,
        off_t value);

    
    void reset_index_children_parent(ToyIndex* begin, ToyIndex* end,
        off_t parent);

    template<class T>
    void node_create(off_t offset, T* node, T* next);

    template<class T>
    void node_remove(T* prev, T* node);

    
    mutable FILE* fp;
    mutable int fp_level;
    void open_file(const char* mode = "rb+") const
    {
        // `rb+` will make sure we can write everywhere without truncating file
        if (fp_level == 0) {
            fp = fopen(path, mode);
            if (fp == nullptr) {
                //int err = GetLastError();
                //err = -1;
                fp = fopen(path, "w+");
                assert(fp);
                fclose(fp);
                fp = fopen(path, mode);
            }
            assert(fp);
        }
        ++fp_level;
    }

    void close_file() const
    {
        if (fp_level == 1)
            fclose(fp);

        --fp_level;
    }

    
    off_t alloc(size_t size)
    {
        off_t slot = meta.slot;
        meta.slot += size;
        return slot;
    }

    off_t alloc(ToyLeafNode* leaf)
    {
        leaf->n = 0;
        meta.leaf_node_num++;
        return alloc(sizeof(ToyLeafNode));
    }

    off_t alloc(ToyInternalNode* node)
    {
        node->n = 1;
        meta.internal_node_num++;
        return alloc(sizeof(ToyInternalNode));
    }

    void unalloc(ToyLeafNode* leaf, off_t offset)
    {
        --meta.leaf_node_num;
    }

    void unalloc(ToyInternalNode* node, off_t offset)
    {
        --meta.internal_node_num;
    }

    
    int map(void* block, off_t offset, size_t size) const
    {
        open_file();
        fseek(fp, offset, SEEK_SET);
        size_t rd = fread(block, size, 1, fp);
        close_file();

        return rd - 1;
    }

    template<class T>
    int map(T* block, off_t offset) const
    {
        return map(block, offset, sizeof(T));
    }

    
    int unmap(void* block, off_t offset, size_t size) const
    {
        open_file();
        fseek(fp, offset, SEEK_SET);
        size_t wd = fwrite(block, size, 1, fp);
        close_file();

        return wd - 1;
    }

    template<class T>
    int unmap(T* block, off_t offset) const
    {
        return unmap(block, offset, sizeof(T));
    }
};



#endif 
