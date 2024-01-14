(in-package :1brc)

(defparameter *data-dir* #p"/Users/david.krentzlin/Private/1brc/data/")

(defun main()
  (let ((file-path (merge-pathnames "100mio.lines" *data-dir*)))
    (try-it file-path)))
