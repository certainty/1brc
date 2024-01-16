(in-package :1brc)

(declaim (optimize (speed 3) (safety 0) (debug 0)))

;;; Some tweaks to experiment and find the best settings
(defparameter *max-unique-stations* (the fixnum 600) "The maximum number of unique stations to expect in the input file")
(defparameter *worker-count* (the fixnum 20) "The number of threads to use for processing")
(defparameter *chunk-size* (the fixnum (* 50 1024 1024)) "The size of the chunks to process per thread")

(defstruct %station
  (max-temp 0.0 :type short-float)
  (min-temp 0.0 :type short-float)
  (count 0 :type fixnum)
  (sum 0.0 :type single-float))

;;; Parse a line of input into a station name and a temperature
(-> parse-line ((vector character *) fixnum) (values string short-float))
(defun parse-line (line line-size)
  "Parses a line of input, which we know has a fixed format of <station>;<simple-float>\n.
   Returns the station name and the simple-float."
  (let ((semi (position #\; (the vector line))))
    (values (subseq (the vector line) 0 semi)
            (parse-simple-float (subseq (the vector line) (1+ semi) (1- line-size))))))

(-> parse-simple-float ((simple-array character *)) short-float)
(defun parse-simple-float (str)
  "Parses a simple floating point number, which is guaranteed to have a single decimal point."
  (let ((signed (char= (aref str 0) #\-)))
    (loop
      :with zero = (char-code #\0)
      :with num :of-type fixnum := 0
      :for c :of-type character :across str
      :unless (or (char= c #\.) (char= c #\-))
        :do (setq num (the fixnum (+ (* num 10) (- (the fixnum (char-code c)) zero))))
      :finally (return (if signed
                           (- (float (/ num 10)))
                           (float (/ num 10)))))))

(-> read-line-from-foreign-ptr-into (t fixnum fixnum simple-string) fixnum)
(defun read-line-from-foreign-ptr-into (ptr ptr-size start buffer)
  "Attempts to read a line from the foreign pointer `ptr'. The line is stored in `buffer'. Returns the number of bytes read"
  (loop :for i :of-type fixnum :from start :below ptr-size
        :for j :of-type fixnum :from 0
        :for byte := (cffi:mem-aref ptr :uchar i)
        :until (= byte 10)              ; newline
        :do (setf (aref buffer j) (code-char byte)) ;; this doesn't work with unicode bytes (we use unsigned char which will read bytes up to 255 instead of 127)
        :finally (return (1+ j))))

(-> compute-segments (t fixnum fixnum) list)
(defun compute-segments (ptr ptr-size minimum-chunk-size)
  "Computes the segments of the foreign pointer `ptr' of size `ptr-size'. The segments are at least `minimum-chunk-size' bytes long. Returns a list of (start end) pairs.
   Segments are aligned at newline characters and will always include the final newline character.
   The segments don't overlap and will be in ascending order.
  "
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
  (loop :for i :of-type fixnum :from start :below ptr-size
        :for byte := (cffi:mem-aref ptr :char i)
        :until (= byte 10)
        :finally (return i)
                 (if (= i ptr-size)
                     (return nil)
                     (return i))))

(-> process-chunk (hash-table simple-string t fixnum fixnum fixnum) hash-table)
(defun process-chunk (hash-tab buffer ptr ptr-size start end)
  (let ((offset start)
        (line-size (the fixnum 0)))
    (prog1 hash-tab
      (loop
        (when (>= (the fixnum offset) end)
          (return hash-tab))
        (setf line-size (the fixnum (read-line-from-foreign-ptr-into ptr ptr-size offset buffer)))
        (incf offset line-size)
        (multiple-value-bind (station-name temperature) (parse-line buffer line-size)
          (let ((record (gethash station-name hash-tab)))
            (cond
              ((null record) (setf (gethash station-name hash-tab)
                                   (make-%station :min-temp temperature :max-temp temperature :count 1 :sum temperature)))
              (t
               (incf (%station-count record))
               (incf (%station-sum record) temperature)
               (when (> temperature (%station-max-temp record))
                 (setf (%station-max-temp record) temperature))
               (when (< temperature (%station-min-temp record))
                 (setf (%station-min-temp record) temperature))))))))))

(-> processing-task (list t fixnum) hash-table)
(defun processing-task (segment ptr ptr-size)
  (let ((buffer (make-string 40))
        (hash-tab (make-process-table)))
    (process-chunk hash-tab buffer ptr ptr-size (car segment) (cdr segment))))

(defun process-file (path)
  (let ((lparallel:*kernel* (lparallel:make-kernel *worker-count*)))
    (mmap:with-mmap (ptr fd ptr-size path)
      (declare (ignore fd))
      (let* ((chunk-size *chunk-size*)
             (segments (compute-segments ptr ptr-size chunk-size))
             (segment-queue (lparallel.queue:make-queue :fixed-capacity (length segments)))
             (channel (lparallel:make-channel))
             (result (make-station-table)))
        (declare (ignorable segment-queue))

        (format t "~%Processing file ~a with ~a segments of size ~a~%" path (length segments) chunk-size)
        (format t "Using ~a workers~%" *worker-count*)

        (prog1 result
          (dolist (segment segments)
            (lparallel:submit-task channel (lambda () (processing-task segment ptr ptr-size))))

          (format t "Waiting for ~a results~%" (length segments))
          (dotimes (i (length segments))
            (a:when-let ((table (lparallel:receive-result channel)))
              (merge-station-tables table result))))))))


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

(defun print-result (station-table)
  (let ((keys (sort (a:hash-table-keys station-table) #'string<)))
    (dolist (key keys)
      (let ((record (gethash key station-table)))
        (format t "~30,a min: ~,2f max: ~,2f mean: ~,2f~%" key (%station-min-temp record) (%station-max-temp record) (/ (%station-sum record) (%station-count record)))))))

