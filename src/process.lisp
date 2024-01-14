(in-package :1brc)

(require :sb-sprof)

(defparameter *max-unique-stations* (the fixnum 10))
(defparameter *worker-count* (the fixnum 32) "The number of threads to use for processing")
(defparameter *chunk-size* (the fixnum (* 50 1024 1024)))

(defstruct %station
  (max-temp 0.0 :type short-float)
  (min-temp 0.0 :type short-float)
  (count 0 :type fixnum)
  (sum 0.0 :type single-float))

(defun make-station-table ()
  (make-hash-table
   :test #'equal
   :size *max-unique-stations*
   :synchronized nil))

(-> make-process-table () hash-table)
(defun make-process-table ()
  (make-hash-table
   :test #'equal
   :size *max-unique-stations*
   :synchronized nil))

(-> merge-station-tables (hash-table hash-table))
(defun merge-station-tables (table-from-proc main-table)
  "Merge `table-from-proc' into `main-table'. This updates the station records in `main-table' with the values from `table-from-proc'.
  Since all values commute, this is easily doable"
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (prog1 main-table
    (loop :for station :being :each :hash-key :of table-from-proc :using (hash-value record)
          :do
             (let ((main-record (gethash station main-table)))
               (cond
                 ((null main-record)
                  (setf (gethash station main-table) record))
                 (t
                  (incf (%station-count main-record) (%station-count record))
                  (when (> (%station-max-temp record) (%station-max-temp main-record))
                    (setf (%station-max-temp main-record) (%station-max-temp record)))
                  (when (< (%station-min-temp record) (%station-min-temp main-record))
                    (setf (%station-min-temp main-record) (%station-min-temp record)))
                  (incf (%station-sum main-record) (%station-sum record))))))))

(-> read-line-from-foreign-ptr-into (t fixnum fixnum simple-string) fixnum)
(defun read-line-from-foreign-ptr-into (ptr ptr-size start buffer)
  "Attempts to read a line from the foreign pointer `ptr'. The line is stored in `buffer'. Returns the number of bytes read"
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (loop :for i :of-type fixnum :from start :below ptr-size
        :for j :of-type fixnum :from 0
        :for byte := (cffi:mem-aref ptr :char i)
        :until (= byte 10) ; newline
        :do (setf (aref buffer j) (code-char byte))
        :finally (return (1+ j))))

(-> compute-segments (t fixnum fixnum) list)
(defun compute-segments (ptr ptr-size minimum-chunk-size)
  "Computes the segments of the foreign pointer `ptr' of size `ptr-size'. The segments are at least `minimum-chunk-size' bytes long. Returns a list of (start end) pairs.
   Segments are aligned at newline characters and will always include the final newline character.
   The segments don't overlap and will be in ascending order.
  "
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (let ((segments (list))
        (start (the (integer 0 *) 0)))

    (when (>= minimum-chunk-size ptr-size)
      (return-from compute-segments (list (cons start ptr-size))))

    (loop :for end :of-type fixnum := (min (+ start minimum-chunk-size) ptr-size)
          :while (< end ptr-size)
          :do (setf end (forward-find-newline ptr ptr-size end))
              (unless (null end)
                (push (cons start end) segments))
              (setf start (1+ end))
          :finally (return (nreverse segments)))))

(-> forward-find-newline (t fixnum fixnum) (or null fixnum))
(defun forward-find-newline (ptr ptr-size start)
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (loop :for i :of-type fixnum :from start :below ptr-size
        :for byte := (cffi:mem-aref ptr :char i)
        :until (= byte 10)
        :finally
           (if (= i ptr-size)
               (return nil)
               (return i))))

(-> process-chunk (hash-table simple-string t fixnum fixnum fixnum))
(defun process-chunk (hash-tab buffer ptr ptr-size start end)
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (let ((offset start)
        (line-size 0))
    (loop
      (when (>= (the fixnum offset) end)
        (return))
      (setf line-size (the fixnum (read-line-from-foreign-ptr-into ptr ptr-size offset buffer)))
      (when (= line-size 0)
        (return))
      (incf offset line-size)
      (multiple-value-bind (station-name temperature) (parse-line buffer)
        (let ((record (gethash station-name hash-tab)))
          (cond
            ((null record)
             (setf (gethash station-name hash-tab)
                   (make-%station :min-temp temperature :max-temp temperature :count 1 :sum temperature)))
            (t
             (incf (%station-count record))
             (when (> temperature (%station-max-temp record))
               (setf (%station-max-temp record) temperature))
             (when (< temperature (%station-min-temp record))
               (setf (%station-min-temp record) temperature))
             (incf (%station-sum record) temperature))))))))

(-> processing-task (lparallel.queue:queue t fixnum) hash-table)
(defun processing-task (segment-queue ptr ptr-size)
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (let  ((buffer (make-string 80))
         (hash-tab (make-process-table)))
    (loop :for segment = (lparallel.queue:try-pop-queue segment-queue)
          :while segment
          :do (process-chunk hash-tab buffer ptr ptr-size (car segment) (cdr segment))
          :finally (return hash-tab))))

(defun process-file (path)
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (let ((lparallel:*kernel* (lparallel:make-kernel *worker-count*)))
    (mmap:with-mmap (ptr fd ptr-size path)
      (declare (ignore fd))
      (let* ((chunk-size (compute-optimal-chunk-size ptr-size))
             (segments (compute-segments ptr ptr-size chunk-size))
             (segment-queue (lparallel.queue:make-queue :fixed-capacity (length segments)))
             (channel (lparallel:make-channel)))

        (format t "~%Processing file ~a with ~a segments of size ~a~%" path (length segments) chunk-size)
        (format t "Using ~a workers~%" *worker-count*)
        (format t "Using ~a bytes of memory~%" *memory-limit*)

        (loop :for segment :in segments
              :do (lparallel.queue:push-queue segment segment-queue))

        (loop :for i :of-type fixnum :from 0 :below *worker-count*
              :do (lparallel:submit-task channel (lambda () (processing-task segment-queue ptr ptr-size))))
        (loop
          :with result = (make-station-table)
          :for i :of-type fixnum :from 0 :below *worker-count*
          :do (let ((table (lparallel:receive-result channel)))
                (when table
                  (merge-station-tables table result)))
          :finally (return result))))))

(defun try-it (path)
  (declare (optimize (speed 3) (safety 0) (debug 0)))
  (process-file path))
