#include <stdlib.h>

#include <list>
#include <algorithm>
using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

#include "toyindexfile.h"


OPERATOR_KEYCMP(ToyIndex)
OPERATOR_KEYCMP(ToyRecord)


template<class T>
inline typename T::child_t begin(T& node) {
    return node.children;
}
template<class T>
inline typename T::child_t end(T& node) {
    return node.children + node.n;
}


inline ToyIndex* find(ToyInternalNode& node, const ToyKeyk& key) {
    if (key) {
        return upper_bound(begin(node), end(node) - 1, key);
    }
    
    if (node.n > 1) {
        return node.children + node.n - 2;
    }
    return begin(node);
}
inline ToyRecord* find(ToyLeafNode& node, const ToyKeyk& key) {
    return lower_bound(begin(node), end(node), key);
}

ToyIndexGOGO::ToyIndexGOGO(const char* p, bool force_empty)
    : fp(NULL), fp_level(0)
{
    memset(path, 0, sizeof(path));
    strcpy(path, p);

    if (!force_empty)
        if (map(&meta, OFFSET_META) != 0)
            force_empty = true;

    if (force_empty) {
        open_file("w+");
        init_from_empty();
        close_file();
    }
}

int ToyIndexGOGO::search(const ToyKeyk& key, ToyValuek* value) const
{
    ToyLeafNode leaf;
    map(&leaf, search_leaf(key));

    ToyRecord* record = find(leaf, key);
    if (record != leaf.children + leaf.n) {
        *value = record->value;

        return keycmp(record->key, key);
    }
    else {
        return -1;
    }
}

int ToyIndexGOGO::search_range(ToyKeyk* left, const ToyKeyk& right,
    ToyValuek* values, size_t max, bool* next) const
{
    if (left == NULL || keycmp(*left, right) > 0)
        return -1;

    off_t off_left = search_leaf(*left);
    off_t off_right = search_leaf(right);
    off_t off = off_left;
    size_t i = 0;
    ToyRecord* b = nullptr, * e = nullptr;

    ToyLeafNode leaf;
    while (off != off_right && off != 0 && i < max) {
        map(&leaf, off);

        if (off_left == off)
            b = find(leaf, *left);
        else
            b = begin(leaf);

        e = leaf.children + leaf.n;
        for (; b != e && i < max; ++b, ++i)
            values[i] = b->value;

        off = leaf.next;
    }

    if (i < max) {
        map(&leaf, off_right);

        b = find(leaf, *left);
        e = upper_bound(begin(leaf), end(leaf), right);
        for (; b != e && i < max; ++b, ++i)
            values[i] = b->value;
    }

    if (next != NULL) {
        if (i == max && b != e) {
            *next = true;
            *left = b->key;
        }
        else {
            *next = false;
        }
    }

    return i;
}

int ToyIndexGOGO::remove(const ToyKeyk& key)
{
    ToyInternalNode parent;
    ToyLeafNode leaf;

    off_t parent_off = search_index(key);
    map(&parent, parent_off);

    ToyIndex* where = find(parent, key);
    off_t offset = where->child;
    map(&leaf, offset);

    if (!binary_search(begin(leaf), end(leaf), key))
        return -1;

    size_t min_n = meta.leaf_node_num == 1 ? 0 : meta.order / 2;
    assert(leaf.n >= min_n && leaf.n <= meta.order);

    ToyRecord* to_delete = find(leaf, key);
    std::copy(to_delete + 1, end(leaf), to_delete);
    leaf.n--;

    if (leaf.n < min_n) {
        bool borrowed = false;
        if (leaf.prev != 0)
            borrowed = borrow_key(false, leaf);

        if (!borrowed && leaf.next != 0)
            borrowed = borrow_key(true, leaf);

        if (!borrowed) {
            assert(leaf.next != 0 || leaf.prev != 0);

            ToyKeyk index_key;

            if (where == end(parent) - 1) {
                assert(leaf.prev != 0);
                ToyLeafNode prev;
                map(&prev, leaf.prev);
                index_key = begin(prev)->key;

                merge_leafs(&prev, &leaf);
                node_remove(&prev, &leaf);
                unmap(&prev, leaf.prev);
            }
            else {
                assert(leaf.next != 0);
                ToyLeafNode next;
                map(&next, leaf.next);
                index_key = begin(leaf)->key;

                merge_leafs(&leaf, &next);
                node_remove(&leaf, &next);
                unmap(&leaf, offset);
            }

            remove_from_index(parent_off, parent, index_key);
        }
        else {
            unmap(&leaf, offset);
        }
    }
    else {
        unmap(&leaf, offset);
    }

    return 0;
}

int ToyIndexGOGO::insert(const ToyKeyk& key, ToyValuek value)
{
    off_t parent = search_index(key);
    off_t offset = search_leaf(parent, key);
    ToyLeafNode leaf;
    map(&leaf, offset);

    if (binary_search(begin(leaf), end(leaf), key))
        return 1;

    if (leaf.n == meta.order) {
        ToyLeafNode new_leaf;
        node_create(offset, &leaf, &new_leaf);

        size_t point = leaf.n / 2;
        bool place_right = keycmp(key, leaf.children[point].key) > 0;
        if (place_right)
            ++point;

        std::copy(leaf.children + point, leaf.children + leaf.n,
            new_leaf.children);
        new_leaf.n = leaf.n - point;
        leaf.n = point;

        if (place_right)
            insert_record_no_split(&new_leaf, key, value);
        else
            insert_record_no_split(&leaf, key, value);

        unmap(&leaf, offset);
        unmap(&new_leaf, leaf.next);

        insert_key_to_index(parent, new_leaf.children[0].key,
            offset, leaf.next);
    }
    else {
        insert_record_no_split(&leaf, key, value);
        unmap(&leaf, offset);
    }

    return 0;
}

int ToyIndexGOGO::update(const ToyKeyk& key, ToyValuek value)
{
    off_t offset = search_leaf(key);
    ToyLeafNode leaf;
    map(&leaf, offset);

    ToyRecord* record = find(leaf, key);
    if (record != leaf.children + leaf.n)
        if (keycmp(key, record->key) == 0) {
            record->value = value;
            unmap(&leaf, offset);

            return 0;
        }
        else {
            return 1;
        }
    else
        return -1;
}

void ToyIndexGOGO::remove_from_index(off_t offset, ToyInternalNode& node,
    const ToyKeyk& key)
{
    size_t min_n = meta.root_offset == offset ? 1 : meta.order / 2;
    assert(node.n >= min_n && node.n <= meta.order);

    ToyKeyk index_key = begin(node)->key;
    ToyIndex* to_delete = find(node, key);
    if (to_delete != end(node)) {
        (to_delete + 1)->child = to_delete->child;
        std::copy(to_delete + 1, end(node), to_delete);
    }
    node.n--;

    if (node.n == 1 && meta.root_offset == offset &&
        meta.internal_node_num != 1)
    {
        unalloc(&node, meta.root_offset);
        meta.height--;
        meta.root_offset = node.children[0].child;
        unmap(&meta, OFFSET_META);
        return;
    }

    if (node.n < min_n) {
        ToyInternalNode parent;
        map(&parent, node.parent);

        bool borrowed = false;
        if (offset != begin(parent)->child)
            borrowed = borrow_key(false, node, offset);

        if (!borrowed && offset != (end(parent) - 1)->child)
            borrowed = borrow_key(true, node, offset);

        if (!borrowed) {
            assert(node.next != 0 || node.prev != 0);

            if (offset == (end(parent) - 1)->child) {
                assert(node.prev != 0);
                ToyInternalNode prev;
                map(&prev, node.prev);

                ToyIndex* where = find(parent, begin(prev)->key);
                reset_index_children_parent(begin(node), end(node), node.prev);
                merge_keys(where, prev, node, true);
                unmap(&prev, node.prev);
            }
            else {
                assert(node.next != 0);
                ToyInternalNode next;
                map(&next, node.next);

                ToyIndex* where = find(parent, index_key);
                reset_index_children_parent(begin(next), end(next), offset);
                merge_keys(where, node, next);
                unmap(&node, offset);
            }

            remove_from_index(node.parent, parent, index_key);
        }
        else {
            unmap(&node, offset);
        }
    }
    else {
        unmap(&node, offset);
    }
}

bool ToyIndexGOGO::borrow_key(bool from_right, ToyInternalNode& borrower,
    off_t offset)
{
    typedef typename ToyInternalNode::child_t child_t;

    off_t lender_off = from_right ? borrower.next : borrower.prev;
    ToyInternalNode lender;
    map(&lender, lender_off);

    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
        child_t where_to_lend, where_to_put;

        ToyInternalNode parent;

        if (from_right) {
            where_to_lend = begin(lender);
            where_to_put = end(borrower);

            map(&parent, borrower.parent);
            child_t where = lower_bound(begin(parent), end(parent) - 1,
                (end(borrower) - 1)->key);
            where->key = where_to_lend->key;
            unmap(&parent, borrower.parent);
        }
        else {
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);

            map(&parent, lender.parent);
            child_t where = find(parent, begin(lender)->key);
            where->key = (where_to_lend - 1)->key;
            unmap(&parent, lender.parent);
        }

        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower.n++;

        reset_index_children_parent(where_to_lend, where_to_lend + 1, offset);
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender.n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

bool ToyIndexGOGO::borrow_key(bool from_right, ToyLeafNode& borrower)
{
    off_t lender_off = from_right ? borrower.next : borrower.prev;
    ToyLeafNode lender;
    map(&lender, lender_off);

    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
        typename ToyLeafNode::child_t where_to_lend, where_to_put;

        if (from_right) {
            where_to_lend = begin(lender);
            where_to_put = end(borrower);
            change_parent_child(borrower.parent, begin(borrower)->key,
                lender.children[1].key);
        }
        else {
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);
            change_parent_child(lender.parent, begin(lender)->key,
                where_to_lend->key);
        }

        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower.n++;

        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender.n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

void ToyIndexGOGO::change_parent_child(off_t parent, const ToyKeyk& o,
    const ToyKeyk& n)
{
    ToyInternalNode node;
    map(&node, parent);

    ToyIndex* w = find(node, o);
    assert(w != node.children + node.n);

    w->key = n;
    unmap(&node, parent);
    if (w == node.children + node.n - 1) {
        change_parent_child(node.parent, o, n);
    }
}

void ToyIndexGOGO::merge_leafs(ToyLeafNode* left, ToyLeafNode* right)
{
    std::copy(begin(*right), end(*right), end(*left));
    left->n += right->n;
}

void ToyIndexGOGO::merge_keys(ToyIndex* where,
    ToyInternalNode& node, ToyInternalNode& next, bool change_where_key)
{
    if (change_where_key) {
        where->key = (end(next) - 1)->key;
    }
    std::copy(begin(next), end(next), end(node));
    node.n += next.n;
    node_remove(&node, &next);
}

void ToyIndexGOGO::insert_record_no_split(ToyLeafNode* leaf,
    const ToyKeyk& key, const ToyValuek& value)
{
    ToyRecord* where = upper_bound(begin(*leaf), end(*leaf), key);
    std::copy_backward(where, end(*leaf), end(*leaf) + 1);

    where->key = key;
    where->value = value;
    leaf->n++;
}

void ToyIndexGOGO::insert_key_to_index(off_t offset, const ToyKeyk& key,
    off_t old, off_t after)
{
    if (offset == 0) {
        ToyInternalNode root;
        root.next = root.prev = root.parent = 0;
        meta.root_offset = alloc(&root);
        meta.height++;

        root.n = 2;
        root.children[0].key = key;
        root.children[0].child = old;
        root.children[1].child = after;

        unmap(&meta, OFFSET_META);
        unmap(&root, meta.root_offset);

        reset_index_children_parent(begin(root), end(root),
            meta.root_offset);
        return;
    }

    ToyInternalNode node;
    map(&node, offset);
    assert(node.n <= meta.order);

    if (node.n == meta.order) {
        ToyInternalNode new_node;
        node_create(offset, &node, &new_node);

        size_t point = (node.n - 1) / 2;
        bool place_right = keycmp(key, node.children[point].key) > 0;
        if (place_right)
            ++point;
            
        if (place_right && keycmp(key, node.children[point].key) < 0)
            point--;

        ToyKeyk middle_key = node.children[point].key;

        std::copy(begin(node) + point + 1, end(node), begin(new_node));
        new_node.n = node.n - point - 1;
        node.n = point + 1;

        if (place_right)
            insert_key_to_index_no_split(new_node, key, after);
        else
            insert_key_to_index_no_split(node, key, after);

        unmap(&node, offset);
        unmap(&new_node, node.next);

        reset_index_children_parent(begin(new_node), end(new_node), node.next);

        insert_key_to_index(node.parent, middle_key, offset, node.next);
    }
    else {
        insert_key_to_index_no_split(node, key, after);
        unmap(&node, offset);
    }
}

void ToyIndexGOGO::insert_key_to_index_no_split(ToyInternalNode& node,
    const ToyKeyk& key, off_t value)
{
    ToyIndex* where = upper_bound(begin(node), end(node) - 1, key);

    std::copy_backward(where, end(node), end(node) + 1);

    where->key = key;
    where->child = (where + 1)->child;
    (where + 1)->child = value;

    node.n++;
}

void ToyIndexGOGO::reset_index_children_parent(ToyIndex* begin, ToyIndex* end,
    off_t parent)
{
    ToyInternalNode node;
    while (begin != end) {
        map(&node, begin->child);
        node.parent = parent;
        unmap(&node, begin->child, SIZE_NO_CHILDREN);
        ++begin;
    }
}

off_t ToyIndexGOGO::search_index(const ToyKeyk& key) const
{
    off_t org = meta.root_offset;
    int height = meta.height;
    while (height > 1) {
        ToyInternalNode node;
        map(&node, org);

        ToyIndex* i = upper_bound(begin(node), end(node) - 1, key);
        org = i->child;
        --height;
    }

    return org;
}

off_t ToyIndexGOGO::search_leaf(off_t index, const ToyKeyk& key) const
{
    ToyInternalNode node;
    map(&node, index);

    ToyIndex* i = upper_bound(begin(node), end(node) - 1, key);
    return i->child;
}

template<class T>
void ToyIndexGOGO::node_create(off_t offset, T* node, T* next)
{
    next->parent = node->parent;
    next->next = node->next;
    next->prev = offset;
    node->next = alloc(next);

    if (next->next != 0) {
        T old_next;
        map(&old_next, next->next, SIZE_NO_CHILDREN);
        old_next.prev = node->next;
        unmap(&old_next, next->next, SIZE_NO_CHILDREN);
    }
    unmap(&meta, OFFSET_META);
}

template<class T>
void ToyIndexGOGO::node_remove(T* prev, T* node)
{
    unalloc(node, prev->next);
    prev->next = node->next;
    if (node->next != 0) {
        T next;
        map(&next, node->next, SIZE_NO_CHILDREN);
        next.prev = node->prev;
        unmap(&next, node->next, SIZE_NO_CHILDREN);
    }
    unmap(&meta, OFFSET_META);
}

void ToyIndexGOGO::init_from_empty()
{
    memset(&meta, 0, sizeof(ToyMeta));
    meta.order = BP_ORDER;
    meta.value_size = sizeof(ToyValuek);
    meta.key_size = sizeof(ToyKeyk);
    meta.height = 1;
    meta.slot = OFFSET_BLOCK;

    ToyInternalNode root;
    root.next = root.prev = root.parent = 0;
    meta.root_offset = alloc(&root);

    ToyLeafNode leaf;
    leaf.next = leaf.prev = 0;
    leaf.parent = meta.root_offset;
    meta.leaf_offset = root.children[0].child = alloc(&leaf);

    unmap(&meta, OFFSET_META);
    unmap(&root, meta.root_offset);
    unmap(&leaf, root.children[0].child);
}

