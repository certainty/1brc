(in-package :cl-user)

(defpackage :1brc.tests
  (:use :cl :lisp-unit2)
  (:local-nicknames (:a :alexandria) (:s :serapeum))
  (:export :run-tests))

(defun run-tests ()
  (run-tests :name "system" :package '(:1brc.tests)))

(defvar sample-file
  "
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
")
