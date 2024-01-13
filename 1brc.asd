(in-package :asdf-user)

(defsystem  "1brc"
  :version "0.1"
  :author "david krentzlin <david.krentzlin@gmail.com>"
  :defsystem-depends-on (:deploy)
  :build-operation "deploy-op"
  :build-pathname "1brc"
  :entry-point "1brc:main"
  :serial t
  :pathname "src/"
  :components
  ((:file "packages")
   (:file "main")))
