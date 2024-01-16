(in-package :asdf-user)


(defsystem  "1brc"
  :version "0.1"
  :author "david krentzlin <david.krentzlin@gmail.com>"
  :defsystem-depends-on (:deploy)
  :build-operation "deploy-op"
  :build-pathname "1brc"
  :entry-point "1brc:main"
  :depends-on (:alexandria
               :serapeum
               :mmap
               :lparallel
               :sb-sprof
               :cffi)
  :in-order-to ((test-op (test-op "1brc/tests")))
  :serial t
  :pathname "src/"
  :components
  ((:file "packages")
   (:file "generate")
   (:file "process")
   (:file "main")))

(defsystem "1brc/tests"
  :depends-on (:1brc :lisp-unit2)
  :serial t
  :pathname "test"
  :components
  ((:file "suite"))
  :perform (test-op (op c)
                    (declare (ignore o c))
                    (uiop:symbol-call :1brc.tests :run-tests)))
