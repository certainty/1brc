(in-package :1brc)

(require :sb-sprof)

(defvar *enable-profiling* nil)

(defun configure-from-env ()
  (let ((configured-worker-count (uiop:getenv "WORKER_COUNT"))
        (configured-chunk-size (uiop:getenv "CHUNK_SIZE"))
        (enable-profiling (equalp (uiop:getenv "ENABLE_PROFILING") "true"))
        (path (uiop:getenv-pathname "DATA_FILE")))
    (when configured-worker-count
      (setf *worker-count* (parse-integer configured-worker-count)))
    (when configured-chunk-size
      (setf *chunk-size* (parse-integer configured-chunk-size)))
    (when enable-profiling
      (setf *enable-profiling* t))

    (unless path
      (error "DATA_FILE environment variable not set."))
    path))

(defun main()
  (let ((file-path (configure-from-env)))
    (if *enable-profiling*
        (sb-sprof:with-profiling (:report :flat :threads :all)
          (run-file file-path))
        (run-file file-path))))

(defun run-file (file-path)
  (let ((result (time (process-file file-path))))
    (print-result result)))
