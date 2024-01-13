(in-package :1brc)

(defparameter *data-dir* #p"/Users/david.krentzlin/Private/1brc/data/")

(defun main()
  (let ((file-path (merge-pathnames "1mio.lines" *data-dir*)))
    (process file-path)))
