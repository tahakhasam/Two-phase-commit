class Constants:
	@property
	def VOTE_COMMIT(self):
		return  b"VOTE_COMMIT"
	
	@property
	def VOTE_ABORT(self):
		return b"VOTE_ABORT"
	
	@property
	def PREPARE(self):
		return b"PREPARE"

	@property
	def GLOBAL_COMMIT(self):
		return b"GLOBAL_COMMIT"

	@property
	def GLOBAL_ABORT(self):
		return b"GLOBAL_ABORT"

	@property
	def DECIDED_TO_COMMIT(self): 
		return b"DECIDED_TO_COMMIT"

	@property
	def RECORDED_COMMIT(self):
		return b"RECORDED_COMMIT"

	@property
	def SUCCESSFUL_COMMIT(self):
		return b"SUCCESSFUL_COMMIT"

	@property
	def SUCCESSFUL_ABORT(self):
		return b"SUCCESSFUL_ABORT"
	