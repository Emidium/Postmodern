
(in-package :postmodern)

(defclass changelog ()
 ((change-number :col-type integer :initarg :change-number :accessor change-number)
  (complete-date :col-type bigint :initarg :complete-date :accessor complete-date)
  (applied-by :col-type (or (varchar 100) db-null) :initarg :applied-by :accessor applied-by)
  (name :col-type (or (varchar 250) db-null) :initarg :name :accessor name))
  (:metaclass dao-class)
  (:keys change-number))

(deftable changelog  (!dao-def))

(defun figure-order-number (somestring) 
 (if (<= 4 (length somestring))
  (parse-integer (subseq somestring 0 4) :junk-allowed t)))

(defclass db-deploy-file () 
 ((full-path :initarg :full-path)
  (order-number :type integer)
  (name :type string )
  (load-type :type string)
  (is-valid)))

(defmethod initialize-instance :after ((deploy-file db-deploy-file) &key)
 (let ((pathname (slot-value deploy-file 'full-path)))
  (setf (slot-value deploy-file 'order-number) (figure-order-number (pathname-name pathname)))
  (setf (slot-value deploy-file 'name) (pathname-name pathname))
  (setf (slot-value deploy-file 'load-type) (pathname-type pathname))
  (setf (slot-value deploy-file 'is-valid) (and 
    (equal nil (slot-value deploy-file 'order-number))
    (or (equal "lisp" (slot-value deploy-file 'load-type)) (equal "sql" (slot-value deploy-file 'load-type)))))
 )
)

(defun db-deploy-files (directory) 
 (sort 
  (remove-if (lambda (deploy-file) (slot-value deploy-file 'is-valid)) 
   (map 'list (lambda (filename) (make-instance 'db-deploy-file :full-path filename)) (cl-fad:list-directory directory)))
    (lambda (x y) (<= (slot-value x 'order-number) (slot-value y 'order-number)))))

(defun db-deploy-run-script (connection-spec db-deploy-script-file)
  "Execute given script against the DB"
  (if (probe-file db-deploy-script-file)
   (with-connection connection-spec (cl-postgres:exec-script *database* (slurp db-deploy-script-file)))
   (error "Could not locate file ~A" db-deploy-script-file)))

(defun db-deploy-drop-all-tables (connection-spec)
 "Drop all tables, be careful"
(with-connection connection-spec
 (let ((table-count (length (list-tables)))) 
  (mapcar 
   (lambda (table-name) 
    (if table-name 
     (execute (:drop-table `,table-name :cascade))
    )
   )
   (list-tables)
  ) 
  table-count
  )
 )
)

(defun db-deploy-migrate (connection-spec db-deploy-dir)
 "Run sql and/or lisp scripts required for migration from the max change number"
 (with-connection connection-spec
  (if (not (table-exists-p "changelog")) (create-table 'changelog))
  (let ((current-changenumber (first (first (query (:select (:max 'change-number) :from 'changelog))))))
   (mapcar (lambda (deploy-dir) (loadif (if (numberp current-changenumber) current-changenumber 0) deploy-dir)) (db-deploy-files db-deploy-dir))
  )
 )
)

(defun loadif (current-changenumber deploy-file) 
 (if (> (slot-value deploy-file 'order-number) current-changenumber)
  (progn
   (if (equal (slot-value deploy-file 'load-type) "lisp")
    (load (slot-value deploy-file 'full-path))
    (exec-script *database* (slurp (slot-value deploy-file 'full-path)))    
   )
   (let ((changelog (make-instance 'changelog :change-number (slot-value deploy-file 'order-number) :complete-date (get-universal-time) :applied-by "db-deploy" :name (slot-value deploy-file 'name))))
    (insert-dao changelog)
    changelog
   )
  )  
 )   
) 
