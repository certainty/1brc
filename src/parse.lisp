(in-package :1brc)

(-> parse-simple-float ((simple-array character *)) short-float)
(defun parse-simple-float (str)
  "Parses a simple floating point number, which is guaranteed to have a single decimal point."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    :with zero = (char-code #\0)
    :with num :of-type fixnum := 0
    :for c :of-type character :across str
    :unless (char= c #\.)
      :do (setq num (the fixnum (+ (* num 10) (- (the fixnum (char-code c)) zero))))
    :finally (return (float (/ num 10)))))

(-> parse-line ((vector character *)) (values string short-float))
(defun parse-line (line)
  "Parses a line of input, which we know has a fixed format of <station>;<simple-float>\n.
   Returns the station name and the simple-float."
  (declare (optimize (speed 3) (safety 0)))
  (let ((semi (position #\; (the vector line))))
    (values (subseq (the vector line) 0 semi)
            (parse-simple-float (subseq (the vector line) (1+ semi))))))
