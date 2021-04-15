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
   uint64_t node_last_index  = (ga->goffset_bytes + ga->nbytes_loc - ga->nbytes_elem) / ga->nbytes_elem;

   // search bounds do not overlap my index set
   if ((ub < node_first_index) || (lb > node_last_index)) return ULLONG_MAX;

   uint64_t first_index = MAX(node_first_index, lb);
   uint64_t last_index  = MIN(node_last_index,  ub);
   uint64_t num_elems   = last_index - first_index;
   uint8_t * first_elem = ga->data + ((first_index - node_first_index) * ga->nbytes_elem);
   uint8_t * last_elem  = ga->data + ((last_index  - node_first_index) * ga->nbytes_elem);

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
   uint64_t node_last_index  = (ga->goffset_bytes + ga->nbytes_loc - ga->nbytes_elem) / ga->nbytes_elem;

   // search bounds do not overlap my index set
   if ((ub < node_first_index) || (lb > node_last_index)) return ULLONG_MAX;

   uint64_t first_index = MAX(node_first_index, lb);
   uint64_t last_index  = MIN(node_last_index,  ub);
   uint64_t num_elems   = last_index - first_index;
   uint8_t * first_elem = ga->data + ((first_index - node_first_index) * ga->nbytes_elem);
   uint8_t * last_elem  = ga->data + ((last_index  - node_first_index) * ga->nbytes_elem);

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
void corank_sorted(const uint64_t index, uint64_t * corank, uint64_t num_bytes,
                   gmt_data_t left,  const uint64_t leftoffset,  const uint64_t leftsize,
                   gmt_data_t right, const uint64_t rightoffset, const uint64_t rightsize, Comparator comp) {
  uint64_t delta;
  uint64_t j    = MIN(index, leftsize);
  uint64_t jlow = (index <= rightsize) ? 0 : index - rightsize;
  uint64_t k    = index - j;
  uint64_t klow = ULLONG_MAX;

  uint64_t * fromleft  = (uint64_t *) malloc(num_bytes);
  uint64_t * fromright = (uint64_t *) malloc(num_bytes);

  for ( ; ;) {
    if (j > 0 && k < rightsize) {
       gmt_get(left,  leftoffset + j - 1, (void *) fromleft,  1);
       gmt_get(right, rightoffset + k,    (void *) fromright, 1);

       if ( comp(fromright, fromleft) ) {
          delta = CEILING(j - jlow, 2);
          klow = k;
          j   -= delta;
          k   += delta;
          continue;
    }   }

    if (k > 0 && j < leftsize) {
       gmt_get(left,  leftoffset + j,      (void *) fromleft,  1);
       gmt_get(right, rightoffset + k - 1, (void *) fromright, 1);

       if ( ! comp(fromright, fromleft) ) {
          delta = CEILING(k - klow, 2);
          jlow  = j;
          j    += delta;
          k    -= delta;
          continue;
    }   }

    break;
  }

  corank[0] = j;
  corank[1] = k;
  free(fromleft);
  free(fromright);
}


template <typename Comparator >
void merge_block_section(uint64_t id, uint64_t num_coworkers, uint64_t num_bytes,
      uint64_t start, uint64_t mid, uint64_t end, gmt_data_t outdata, gmt_data_t indata, Comparator comp) {

  uint64_t i[2], lower[2], upper[2];
  i[0] = id       * (end - start) / num_coworkers;
  i[1] = (id + 1) * (end - start) / num_coworkers;

  corank_sorted(i[0], lower, num_bytes, indata, start, mid - start, indata, mid, end - mid, comp);
  corank_sorted(i[1], upper, num_bytes, indata, start, mid - start, indata, mid, end - mid, comp);

  uint64_t leftsize    = upper[0] - lower[0];
  uint64_t rightsize   = upper[1] - lower[1];

  uint64_t start_left  = start + lower[0];
  uint64_t end_left    = start_left + leftsize;
  uint64_t start_right = mid + lower[1];
  uint64_t end_right   = start_right + rightsize;

// if one side is empty, copy the other side and return
  if      (rightsize == 0) {gmt_memcpy(indata, start_left,  outdata, start + i[0], leftsize);  return; }
  else if (leftsize  == 0) {gmt_memcpy(indata, start_right, outdata, start + i[0], rightsize); return; }

// get left and right side
  uint64_t num_elems = leftsize + rightsize;
  uint8_t * buffer_in = (uint8_t *) malloc(num_elems * num_bytes);
  uint8_t * buffer_out = (uint8_t *) malloc(num_elems * num_bytes);

  uint8_t * outPtr = buffer_out;
  uint8_t * leftPtr = buffer_in;
  uint8_t * leftEnd = leftPtr + leftsize * num_bytes;
  uint8_t * rightPtr = leftEnd;
  uint8_t * rightEnd = rightPtr + rightsize * num_bytes;

  gmt_get(indata, start_left, (void *) leftPtr, leftsize);
  gmt_get(indata, start_right, (void *) rightPtr, rightsize);

// merge left and right side
  while ( (leftPtr < leftEnd) || (rightPtr < rightEnd) ) {
     if (leftPtr == leftEnd) {
        memcpy(outPtr, rightPtr, (rightEnd - rightPtr));
        rightPtr = rightEnd;
     } else if (rightPtr == rightEnd) {
        memcpy(outPtr, leftPtr, (leftEnd - leftPtr));
        leftPtr = leftEnd;
     } else if ( comp((uint64_t *) leftPtr, (uint64_t *) rightPtr) ) {
        memcpy(outPtr, leftPtr, num_bytes);
        outPtr += num_bytes; leftPtr += num_bytes;
     } else {
        memcpy(outPtr, rightPtr, num_bytes);
        outPtr += num_bytes; rightPtr += num_bytes;
  }  }

  gmt_put(outdata, start + i[0], (void *) buffer_out, num_elems);

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
