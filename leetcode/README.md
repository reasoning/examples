## Submissions to easy leetcode.com Problems

* Most submissions are faster than ~95% and use less memory than ~95% of all other submissions.

* Some submissions acheived 100% performance and 0ms execution time (which shows the failure of high resolution timers on virtual machines).


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/92/array/727/

Given a sorted array nums, remove the duplicates in-place such that each element appears only once and returns the new length.

Do not allocate extra space for another array, you must do this by modifying the input array in-place with O(1) extra memory.

Clarification:

Confused why the returned value is an integer but your answer is an array?

Note that the input array is passed in by reference, which means a modification to the input array will be known to the caller as well.

```
class Solution {
public:
    int removeDuplicates(vector<int>& nums) {
                        
        int size = (int)nums.size();        
        if (size == 0) return 0;
        
        int i=0;
        for (int j=1;j<size;++j)
            if (nums[i]!=nums[j])
                nums[++i]=nums[j];
        
        // We should resize nums to make this function more robust
        //nums.resize(i);
        return i+1;
    }
};

```


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/94/trees/555/
Given the root of a binary tree, return its maximum depth.

A binary tree's maximum depth is the number of nodes along the longest path from the root node down to the farthest leaf node.

 

```
/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode() : val(0), left(nullptr), right(nullptr) {}
 *     TreeNode(int x) : val(x), left(nullptr), right(nullptr) {}
 *     TreeNode(int x, TreeNode *left, TreeNode *right) : val(x), left(left), right(right) {}
 * };
 */
class Solution {
public:
    int maxDepth(TreeNode* root) {
        
        return maxDepthRecursive(root,1);
    }
    
    int maxDepthRecursive(TreeNode * node, int depth) {
        
        if (!node) return 0;
        
        int lhs=0;
        int rhs=0;
        if (node->left)
            lhs = maxDepthRecursive(node->left,depth+1);
        if (node->right)
            rhs = maxDepthRecursive(node->right,depth+1);
        
        int child = std::max(lhs,rhs);
        return std::max(child,depth);
    }
    
};

```


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/94/trees/625/

Given the root of a binary tree, determine if it is a valid binary search tree (BST).

A valid BST is defined as follows:

The left subtree of a node contains only nodes with keys less than the node's key.
The right subtree of a node contains only nodes with keys greater than the node's key.
Both the left and right subtrees must also be binary search trees.

```
/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode() : val(0), left(nullptr), right(nullptr) {}
 *     TreeNode(int x) : val(x), left(nullptr), right(nullptr) {}
 *     TreeNode(int x, TreeNode *left, TreeNode *right) : val(x), left(left), right(right) {}
 * };
 */
class Solution {
public:
    bool isValidBST(TreeNode* root) {
        vector<int> values;
        inorderTraversal(root,values);
        
        for (int i=0;i<(int)values.size()-1;++i) {
            if (values[i+1] <= values[i])
                return false;            
        }
        
        return true;
            
    }
    
    void inorderTraversal(TreeNode * node, vector<int> & values) {
        
        if (!node) return;
        
        inorderTraversal(node->left,values);
        values.push_back(node->val);
        inorderTraversal(node->right,values);        
    }
        
};

```

### https://leetcode.com/explore/interview/card/top-interview-questions-easy/94/trees/627/

Given the root of a binary tree, check whether it is a mirror of itself (i.e., symmetric around its center).

* This can also be solved using recursion in much less code, but then of course you can overflow the stack on a large tree, so i deliberately chose an iterator based solution here.

```
Given the root of a binary tree, check whether it is a mirror of itself (i.e., symmetric around its center).

/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode() : val(0), left(nullptr), right(nullptr) {}
 *     TreeNode(int x) : val(x), left(nullptr), right(nullptr) {}
 *     TreeNode(int x, TreeNode *left, TreeNode *right) : val(x), left(left), right(right) {}
 * };
 */

class Solution {
public:
    
    enum Direction {LEFT = -1,RIGHT = 1};
    
    struct Iter {
        
        vector<TreeNode*> Stack;
        TreeNode * Node;
        int Dir;
                
        Iter(TreeNode * node, int dir):Node(node),Dir(dir) {}
        
        void push() {
            Stack.push_back(Node);
        }
        
        void pop() {
            if (Stack.size() > 0) {
                Node = Stack.back();
                Stack.pop_back();
            } else {
                Node = 0;
            }        
        }
        
        bool next(int & value) {
            
            if (!Node) return false;
                
            // Implement two symmetric iterations, doesnt really matter if its inorder
            // preorder, postorder, or some other custom order (which it is) provided
            // that they mirror each other
            if (Dir < 0) {                
                if (Node->left) {
                    push();
                    Node = Node->left;
                } 
                else
                if (Node->right) {
                    push();
                    Node = Node->right;                    
                } 
                else {
                    while (Node) {
                        pop();
                        if (Node && Node->right)
                        {
                            Node = Node->right;
                            break;
                        }
                    }                
                }            
            }
            else
            if (Dir > 0) {
                if (Node->right) {
                    push();
                    Node = Node->right;
                }
                else
                if (Node->left) {
                    push();
                    Node = Node->left;
                }
                else {
                    while (Node) {
                        pop();
                        if (Node && Node->left)
                        {
                            Node = Node->left;
                            break;
                        }                        
                    }
                }  
                
            }
            else {
                return false;
            }

            if (Node)
                value = Node->val;
            
            return Node != 0;                    
        }
    };
    
    bool isSymmetric(TreeNode* root) {
        // Well the obvious solution is just to traverse left and right nodes
        // at the same time using an iterator type object, and then compare for
        // symmetry.  Iteration order can just flip from left to right depeding
        // on where we start. 
        
        Iter left(root,LEFT);
        Iter right(root,RIGHT);
        
        int lhs=0;
        int rhs=0;
        
        bool sym = true;
        while(true) {
            
            bool ll = left.next(lhs);
            bool rr = right.next(rhs);
        
            if (ll && rr) {
                if (lhs != rhs) {
                    sym = false;
                    break;
                }
                
            } else {
                sym = ll == rr;
                break;
            }
        }            
        
        return sym;
    }
    
    
    
};
```


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/94/trees/628/

Given the root of a binary tree, return the level order traversal of its nodes' values. (i.e., from left to right, level by level).


```
/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode() : val(0), left(nullptr), right(nullptr) {}
 *     TreeNode(int x) : val(x), left(nullptr), right(nullptr) {}
 *     TreeNode(int x, TreeNode *left, TreeNode *right) : val(x), left(left), right(right) {}
 * };
 */
class Solution {
public:
    
    void levelSum(TreeNode * node, int level, vector<vector<int>> &levels) {
        if (!node) return;
            
        if ((int)levels.size() < level+1)
            levels.push_back(vector<int>());
        
        levels[level].push_back(node->val);
        
        levelSum(node->left,level+1,levels);
        levelSum(node->right,level+1,levels);        
    }
    
    
    vector<vector<int>> levelOrder(TreeNode* root) {
        
        vector<vector<int>> levels;
        levelSum(root,0,levels);      
        return std::move(levels);
    }
    
};

```


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/94/trees/631/

Given an integer array nums where the elements are sorted in ascending order, convert it to a height-balanced binary search tree.

A height-balanced binary tree is a binary tree in which the depth of the two subtrees of every node never differs by more than one.

 
```
/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode() : val(0), left(nullptr), right(nullptr) {}
 *     TreeNode(int x) : val(x), left(nullptr), right(nullptr) {}
 *     TreeNode(int x, TreeNode *left, TreeNode *right) : val(x), left(left), right(right) {}
 * };
 */
class Solution {
public:
    
   
    TreeNode * splitBST(vector<int>& nums, int from, int to) {
        
        if (from == to) {
            return new TreeNode(nums[from]);
        }
        else
        if (from < to) {
            // Pivot is just the average, or half from plus half to, i.e. in cartesian space
            // the middle point between two points.
            int pivot = (int)(((long long)from+(long long)to)>>1);
            TreeNode * root = new TreeNode(nums[pivot]);
            root->left = splitBST(nums,from,pivot-1);
            root->right = splitBST(nums,pivot+1,to);
            return root;
        }
            
        return 0;
    }
    
    TreeNode* sortedArrayToBST(vector<int>& nums) {
        TreeNode * root = 0;
        
        // Well, since were insterting from a sorted array, the easy way to ensure balance is 
        // just to partition the array as if we were doing a quick sort. The alternative would
        // be just to implement one of many tree algorithms like avl/splay etc to rotate and 
        // or weight branches.
        if ((int)nums.size() > 0) {
            int from = 0;
            int to = nums.size()-1;
            root = splitBST(nums,from,to);
        }
        
        return root;
    }
};

```


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/96/sorting-and-searching/587/

Given two sorted integer arrays nums1 and nums2, merge nums2 into nums1 as one sorted array.

The number of elements initialized in nums1 and nums2 are m and n respectively. You may assume that nums1 has a size equal to m + n such that it has enough space to hold additional elements from nums2.

```
class Solution {
public:
    void merge(vector<int>& nums1, int m, vector<int>& nums2, int n) {

        // Merge sort
        for (int nn=0;nn<n;++nn)
        {
            int mm=0;
            for (;mm<m+n;++mm)
            {
                 if (mm < m && nums1[mm] <= nums2[nn])
                     continue;
            
                nums1.insert(nums1.begin()+mm,nums2[nn]);
                ++m;
                break;
            }
        }
        
        // Trim the trailing zeros, this problem is poorly specified
        nums1.resize(m);
            
    }
};
```


### https://leetcode.com/explore/interview/card/top-interview-questions-easy/96/sorting-and-searching/774/

You are a product manager and currently leading a team to develop a new product. Unfortunately, the latest version of your product fails the quality check. Since each version is developed based on the previous version, all the versions after a bad version are also bad.

Suppose you have n versions [1, 2, ..., n] and you want to find out the first bad one, which causes all the following ones to be bad.

You are given an API bool isBadVersion(version) which returns whether version is bad. Implement a function to find the first bad version. You should minimize the number of calls to the API.


```
// The API isBadVersion is defined for you.
// bool isBadVersion(int version);

class Solution {
public:
    int firstBadVersion(int n) {
        // Well, firstly just recognise this is a basic linear search vs
        // binary search problem, i.e. we could start at 0 and go up to n
        // but thats very inefficient, so we bi-sect again using the same
        // pivot style that we would use in quick sort etc
        int from=0;
        int to=n;
        
        // Using long long avoids integer overflow
        while (from < to) {
            int pivot = (int)(((long long)from+(long long)to)>>1);
            
            if (isBadVersion(pivot))                
                to = pivot;
            else
                from = pivot+1;                          
        }
        
        return from;
    }
};


```
