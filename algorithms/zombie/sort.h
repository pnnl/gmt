#ifndef SORT_H
#define SORT_H

#define SORT_COLS 16

typedef std::vector <uint64_t> uintVec_t;
/**
 * Sort elements of a gmt_array according to columns in ascending order.
 * Returns a new gmt_array.
*/ 
gmt_data_t gmt_sort(gmt_data_t data, uint64_t * columns);

/*
 * Return index of first element of gmt array NOT less than value when compared according to columns.
 * Returns 0 if value is less than first element of gmt array.
 * Return number of elements if value is greater than last element of gmt array
*/
uint64_t gmt_lower_bound(uintVec_t & value, gmt_data_t data, uint64_t * columns);
uint64_t gmt_lower_bound_limit(uintVec_t & , gmt_data_t , uint64_t * , uint64_t lb, uint64_t ub);

/*
 * Return index of first element of gmt array greater than value when compared according to columns.
 * Return number of elements if value is greater than last element of gmt array
*/
uint64_t gmt_upper_bound(uintVec_t & value, gmt_data_t data, uint64_t * columns);
uint64_t gmt_upper_bound_limit(uintVec_t & , gmt_data_t , uint64_t * , uint64_t lb, uint64_t ub);

/*
 * Return the range of elements of gmt array that are equal to value when compared according to columns.
 * If no element is equal to value, range elements equal index of first row greater than value.  If all
 * elements are less than value, range elements equal number of elements.
*/
std::pair <uint64_t, uint64_t> gmt_equal_range(uintVec_t &, gmt_data_t, uint64_t * columns);
std::pair <uint64_t, uint64_t> gmt_equal_range_limit(uintVec_t &, gmt_data_t, uint64_t * columns, uint64_t , uint64_t);

/*
 * Check that the elements of gmt array are sorted in ascending order according to columns.
*/
void checkSort(std::string name, gmt_data_t data, uint64_t * columns);

/********** COMPARATOR **********/
// compare the columns of two rows
static std::function <bool(const uint64_t *, const uint64_t *)>
get_comparator(uint64_t * columns) {
  auto cmp = [columns](const uint64_t * a, const uint64_t * b) -> bool {
    for (uint64_t * col = columns; * col != ULLONG_MAX; col ++) {
      if (a[* col] < b[* col]) return true;
      else if (a[* col] > b[* col]) return false;
    }

    return false;
  };

  return cmp;
}


/********** FIND METHODS **********/
template <typename Comparator>
uint64_t lowerbound(uint64_t * keyRow, gmt_data_t data, Comparator comp, uint64_t lb, uint64_t ub) {
   gentry_t * ga  = mem_get_gentry(data);
   uint64_t node_first_index = ga->goffset_bytes / ga->nbytes_elem;
   uint64_t node_last_index  = (ga->goffset_bytes + ga->nbytes_loc) / ga->nbytes_elem;

   // search bounds do not overlap my index set
   if ((ub < node_first_index) || (lb >= node_last_index)) return ULLONG_MAX;

   uint64_t first_index = MAX(node_first_index, lb);
   uint64_t last_index  = MIN(node_last_index,  ub);
   uint64_t num_elems   = last_index - first_index;
   uint8_t * first_elem = ga->data + ((first_index - node_first_index) * ga->nbytes_elem);
   uint8_t * last_elem  = ga->data + ((last_index  - node_first_index - 1) * ga->nbytes_elem);

   if ( ! comp((uint64_t *) first_elem, keyRow) ) {           // IF key <= first element

      if (first_index == lb) {     // if first index is lower bound, return first index
         return first_index;
      } else {                     // else compare with previous element
         std::vector <uint8_t> value(ga->nbytes_elem);                          // previous element may be on ...
         gmt_get(data, first_index - 1, (void *) value.data(), 1);              // another processor, so use gmt_get
         if ( comp((uint64_t *) value.data(), keyRow) ) return first_index;     // if element < key, return first_index
      }

   } else if ( ! comp((uint64_t *) last_elem, keyRow) ) {     // ELSE IF key <= last element, search for key
      
      while (num_elems > 0) {
         uint64_t step = num_elems / 2;
         uint8_t * ptr = first_elem + (step * ga->nbytes_elem);

         if ( comp((uint64_t *) ptr, keyRow) ) {
            ptr += ga->nbytes_elem;
            num_elems -= step + 1;
            first_elem = ptr;
         } else {
            num_elems = step;
      }  }

      return (ga->goffset_bytes + (first_elem - ga->data)) / ga->nbytes_elem;
   }

   return ULLONG_MAX;
}


template <typename Comparator>
uint64_t upperbound(uint64_t * keyRow, gmt_data_t data, Comparator comp, uint64_t lb, uint64_t ub) {
   gentry_t * ga  = mem_get_gentry(data);
   uint64_t node_first_index = ga->goffset_bytes / ga->nbytes_elem;
   uint64_t node_last_index  = (ga->goffset_bytes + ga->nbytes_loc) / ga->nbytes_elem;

   // search bounds do not overlap my index set
   if ((ub < node_first_index) || (lb >= node_last_index)) return ULLONG_MAX;

   uint64_t first_index = MAX(node_first_index, lb);
   uint64_t last_index  = MIN(node_last_index,  ub);
   uint64_t num_elems   = last_index - first_index;
   uint8_t * first_elem = ga->data + ((first_index - node_first_index) * ga->nbytes_elem);
   uint8_t * last_elem  = ga->data + ((last_index  - node_first_index - 1) * ga->nbytes_elem);

   if ( comp(keyRow, (uint64_t *) first_elem) ) {           // IF key < first element

      if (first_index == lb) {     // if first index is lower bound, return first index
         return first_index;
      } else {                     // else compare with previous element
         std::vector <uint8_t> value(ga->nbytes_elem);                          // previous element may be on ...
         gmt_get(data, first_index - 1, (void *) value.data(), 1);              // another processor, so use gmt_get
         if ( ! comp(keyRow, (uint64_t *) value.data()) ) return first_index;   // if element <= key, return first_index
      }

   } else if ( comp(keyRow, (uint64_t *) last_elem) ) {     // ELSE IF key < last element on this node
      
      while (num_elems > 0) {
         uint64_t step = num_elems / 2;
         uint8_t * ptr = first_elem + (step * ga->nbytes_elem);

         if ( ! comp(keyRow, (uint64_t *) ptr) ) {
            ptr += ga->nbytes_elem;
            num_elems -= step + 1;
            first_elem = ptr;
         } else {
            num_elems = step;
      }  }

      return (ga->goffset_bytes + (first_elem - ga->data)) / ga->nbytes_elem;
   }

   return ULLONG_MAX;
}

/********** SORT **********/
template <typename Comparator >
void merge_block_section(uint64_t id, uint64_t num_coworkers, uint64_t num_bytes, uint64_t start,
      uint64_t mid, uint64_t end, gmt_data_t outdata, gmt_data_t indata, uint64_t * columns, Comparator comp) {

  std::vector <uint64_t> X(num_bytes / sizeof(uint64_t));
  std::vector <uint64_t> Y(num_bytes / sizeof(uint64_t));
  uint64_t blocksize_left  = CEILING(mid - start, num_coworkers);
  uint64_t blocksize_right = CEILING(end - mid, num_coworkers);

  uint64_t left_start  = start + blocksize_left  * id;              // start of LHS
  uint64_t right_start = mid   + blocksize_right * id;              // start of RHS block;
  uint64_t left_end  = MIN(left_start  + blocksize_left,  mid);     // end   of LHS
  uint64_t right_end = MIN(right_start + blocksize_right, end);     // end   of RHS block;

  // if left_start points into a sequence of equal elements, move to end of sequence
  if (left_start > start) {
     gmt_get(indata, left_start - 1, (void *) X.data(), 1);
     gmt_get(indata, left_start,     (void *) Y.data(), 1);
     if (! comp(X.data(), Y.data())) left_start = gmt_upper_bound_limit(Y, indata, columns, left_start, mid);
  }

  // if left_end points into a sequence of equal elements, move to end of sequence
  if (left_end < mid) {
     gmt_get(indata, left_end - 1, (void *) X.data(), 1);
     gmt_get(indata, left_end    , (void *) Y.data(), 1);
     if (! comp(X.data(), Y.data())) left_end = gmt_upper_bound_limit(Y, indata, columns, left_end - 1, mid);
  }

  // set right_start to first element > element before left_start
  if (right_start != mid) {
     gmt_get(indata, left_start - 1, (void *) X.data(), 1);
     right_start = gmt_upper_bound_limit(X, indata, columns, mid, end);
  }

  // set right_end to first element > element before left_end
  if (right_end != end) {
     gmt_get(indata, left_end - 1, (void *) Y.data(), 1);
     right_end = gmt_upper_bound_limit(Y, indata, columns, right_start, end);
  }

  uint64_t leftsize  = left_end  - left_start;
  uint64_t rightsize = right_end - right_start;

  uint64_t num_elems = leftsize + rightsize;
  uint64_t outdata_start = left_start + (right_start - mid);

  uint64_t left_index = left_start;
  uint64_t right_index = right_start;

// if one side is empty, copy the other side and return
  if      (rightsize == 0) {gmt_memcpy(indata, left_start,  outdata, outdata_start, leftsize);  return; }
  else if (leftsize  == 0) {gmt_memcpy(indata, right_start, outdata, outdata_start, rightsize); return; }

// get left and right side
  uint8_t * buffer_in = (uint8_t *) malloc(num_elems * num_bytes);
  uint8_t * buffer_out = (uint8_t *) malloc(num_elems * num_bytes);

  uint8_t * outPtr   = buffer_out;
  uint8_t * leftPtr  = buffer_in;
  uint8_t * leftEnd  = leftPtr + leftsize * num_bytes;
  uint8_t * rightPtr = leftEnd;
  uint8_t * rightEnd = rightPtr + rightsize * num_bytes;

  gmt_get(indata, left_start, (void *) leftPtr, leftsize);
  gmt_get(indata, right_start, (void *) rightPtr, rightsize);

// merge left and right side
  while ( (leftPtr < leftEnd) || (rightPtr < rightEnd) ) {
     if (leftPtr == leftEnd) {
        memcpy(outPtr, rightPtr, (rightEnd - rightPtr));
        rightPtr = rightEnd;
     } else if (rightPtr == rightEnd) {
        memcpy(outPtr, rightPtr, (rightEnd - rightPtr));
        memcpy(outPtr, leftPtr, (leftEnd - leftPtr));
        leftPtr = leftEnd;
     } else if ( comp((uint64_t *) leftPtr, (uint64_t *) rightPtr) ) {
        memcpy(outPtr, leftPtr, num_bytes);
        outPtr += num_bytes; leftPtr += num_bytes;
        left_index ++;
     } else {
        memcpy(outPtr, rightPtr, num_bytes);
        outPtr += num_bytes; rightPtr += num_bytes;
        right_index ++;
  }  }

  gmt_put(outdata, outdata_start, (void *) buffer_out, num_elems);

  free(buffer_in);
  free(buffer_out);
}


/********** CHECK SORT **********/
template <typename Comparator>
void checksort(gmt_data_t data, Comparator comp) {
  gentry_t * ga = mem_get_gentry(data);
  uint64_t nbytes_elem = ga->nbytes_elem;
  uint64_t index = ga->goffset_bytes / nbytes_elem;
  uint64_t num_cols = nbytes_elem / sizeof(uint64_t);

  uint64_t * current_row = (uint64_t *) ga->data;
  uint64_t * last_row    = (uint64_t *) (ga->data + ga->nbytes_loc - nbytes_elem);

  if (gmt_node_id() != 0) {                             // if not node 0, check first element
     std::vector <uint64_t> previous_row(num_cols);
     gmt_get(data, index - 1, previous_row.data(), 1);
     if ( comp(current_row, previous_row.data()) ) printf("error: row %lu\n", index);
  }

  while (current_row < last_row) {     // check the rest of the elements
    if ( comp(current_row + num_cols, current_row) ) printf("error: row %lu\n", index);
    current_row += num_cols;
    index ++;
} }
  
#endif /* SORT_H */
