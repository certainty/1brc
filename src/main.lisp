(in-package :1brc)

(defparameter *data-dir* #p"/Users/david.krentzlin/Private/1brc/data/")

(defun main()
  (let ((file-path (merge-pathnames "100mio.lines" *data-dir*))
        (configured-worker-count (uiop:getenv "WORKER_COUNT"))
        (configured-chunk-size (uiop:getenv "CHUNK_SIZE")))

    (when configured-worker-count
      (setf *worker-count* (parse-integer configured-worker-count)))

    (when configured-chunk-size
      (setf *chunk-size* (parse-integer configured-chunk-size)))

    (process-file file-path)))
