(in-package :1brc)

(defun configure-from-env ()
  (let ((configured-worker-count (uiop:getenv "WORKER_COUNT"))
        (configured-chunk-size (uiop:getenv "CHUNK_SIZE"))
        (path (uiop:getenv-pathname "DATA_FILE")))
    (when configured-worker-count
      (setf *worker-count* (parse-integer configured-worker-count)))
    (when configured-chunk-size
      (setf *chunk-size* (parse-integer configured-chunk-size)))
    (unless path
      (error "DATA_FILE environment variable not set."))
    path))

(defun main()
  (let* ((file-path (configure-from-env))
         (result (time (process-file file-path))))
    (format t "~&Result: ~A~%" result)
    (print-result result)))
