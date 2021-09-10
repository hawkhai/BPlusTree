#ifndef ToyIndexGOGO_HH
#define ToyIndexGOGO_HH

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

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

#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(meta_t)
#define SIZE_NO_CHILDREN sizeof(leaf_node_t) - BP_ORDER * sizeof(ToyRecord)


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
} meta_t;


struct ToyIndex {
    ToyKeyk key;
    off_t child; 
};


struct internal_node_t {
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


struct leaf_node_t {
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
    meta_t get_meta() const {
        return meta;
    };


private:
    char path[512];
    meta_t meta;

    
    void init_from_empty();

    
    off_t search_index(const ToyKeyk& key) const;

    
    off_t search_leaf(off_t index, const ToyKeyk& key) const;
    off_t search_leaf(const ToyKeyk& key) const
    {
        return search_leaf(search_index(key), key);
    }

    
    void remove_from_index(off_t offset, internal_node_t& node,
        const ToyKeyk& key);

    
    bool borrow_key(bool from_right, internal_node_t& borrower,
        off_t offset);

    
    bool borrow_key(bool from_right, leaf_node_t& borrower);

    
    void change_parent_child(off_t parent, const ToyKeyk& o, const ToyKeyk& n);

    
    void merge_leafs(leaf_node_t* left, leaf_node_t* right);

    void merge_keys(ToyIndex* where, internal_node_t& left,
        internal_node_t& right, bool change_where_key = false);

    
    void insert_record_no_split(leaf_node_t* leaf,
        const ToyKeyk& key, const ToyValuek& value);

    
    void insert_ToyKeyko_index(off_t offset, const ToyKeyk& key,
        off_t value, off_t after);
    void insert_ToyKeyko_index_no_split(internal_node_t& node, const ToyKeyk& key,
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
        if (fp_level == 0)
            fp = fopen(path, mode);

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

    off_t alloc(leaf_node_t* leaf)
    {
        leaf->n = 0;
        meta.leaf_node_num++;
        return alloc(sizeof(leaf_node_t));
    }

    off_t alloc(internal_node_t* node)
    {
        node->n = 1;
        meta.internal_node_num++;
        return alloc(sizeof(internal_node_t));
    }

    void unalloc(leaf_node_t* leaf, off_t offset)
    {
        --meta.leaf_node_num;
    }

    void unalloc(internal_node_t* node, off_t offset)
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
