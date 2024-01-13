(in-package :1brc)

(require :sb-sprof)

(eval-when (:compile-toplevel)
  (declaim (optimize (speed 3) (safety 0) (debug 0))))

(defstruct %station
  (max-temp 0.0 :type short-float)
  (min-temp 0.0 :type short-float)
  (count 0 :type fixnum)
  (sum 0.0 :type single-float))

(defun make-station-table ()
  (make-hash-table :test #'equal))

(defun process (path)
  (with-open-file (stream path)
    (time
     (sb-sprof:with-profiling (:report :flat :threads :all)
       (%process-simple stream)))))

(-> forward (hash-table function) t)
(defun forward (hash-tab send-fn)
  (declare (optimize (speed 3) (safety 0) (debug 0)))

  (funcall send-fn hash-tab))

(defun process-line (line line-no hash-tab send-fn)
  (declare (ignore send-fn line-no) (optimize (speed 3) (safety 0) (debug 0)))

  #+print-progress
  (when (zerop (mod line-no 100000))
    (format t "~&~A" line-no))

  (let* ((parts (split-sequence:split-sequence #\; line))
         (station (first parts))
         (temp-value (the short-float (parse-float:parse-float (second parts))))
         (record (gethash station hash-tab)))
    (cond
      ((null record)
       (setf (gethash station hash-tab)
             (make-%station :min-temp temp-value :max-temp temp-value :count 1 :sum temp-value)))
      (t
       (incf (%station-count record))
       (when (> temp-value (%station-max-temp record))
         (setf (%station-max-temp record) temp-value))
       (when (< temp-value (%station-min-temp record))
         (setf (%station-min-temp record) temp-value))
       (incf (%station-sum record) temp-value))))
  hash-tab)

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

(defun %process-simple (stream)
  (stream-par-procs:process
   stream
   #'process-line
   :num-of-procs 4
   :init-proc-state-fn #'make-station-table
   :init-collect-state-fn #'make-station-table
   :process-end-of-stream-hook-fn #'forward
   :collect-fn #'merge-station-tables))
