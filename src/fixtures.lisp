(in-package :1brc)

(defparameter *base-stations* (vector "Hamburg" "Bulawayo" "Parembang" "St. John's" "Istanbul" "Roseau" "Bridgetown"))

(defun generate-file (output-path &key (number-of-rows 1000))
  (with-open-file (stream output-path
                          :direction :output
                          :if-exists :supersede
                          :if-does-not-exist :create)
    (loop :for i :from 1 :to number-of-rows
          :do (format stream "~a~%" (random-line)))))

(defun random-line ()
  "Generates a random line of the format <station>;<float> where float follows nn.n"
  (let ((station (aref *base-stations* (random (length *base-stations*))))
        (float (format nil "~a.~a" (random 200) (random 10))))
    (concatenate 'string station ";" float)))
