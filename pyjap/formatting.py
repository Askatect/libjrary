import logger
import logging
import utilities as utilities
import json
import numpy as np
import pandas as pd

# class QueryLevel:
# 	def __init__(self, level: int):
# 		self.level = level
# 		self.indent = 0
# 		self._set_ = False
# 		self._on_ = False
# 		self._select_ = False
# 		self._from_ = False
# 		self._values_ = False
# 		self._case_ = False
# 		self._join_ = False
# 		self._operator_ = False
# 		self._where_ = False

# 	def __repr__(self):
# 		return f"QueryLevel {self.level} with indent {self.indent}."
	
# 	def increment(self):
# 		self.indent += 1

# 	def decrement(self):
# 		self.indent -= 1

# 	def operator_ready(self):
# 		return (self._on_ or self._where_) and (self._operator_ == False)
	
# 	def aliasing(self, line):
# 		start = line.split(' ')[0]
# 		end = line.rstrip().split(' ')[-1]
# 		try:
# 			last = end[-1]
# 		except:
# 			last = end
# 		return ((self._select_ and last == ',') or self._join_ or start == "FROM") and ("AS" in line and "[" not in end)

# 	def format_line(self, line, indents: int = 0):
# 		if self._on_ == True and self._set_ == True:
# 			return line
# 		indents = self.level + self.indent + indents
# 		return '\n' + indents*'\t' + line

# def reduce(string: str):
# 	for char in [('\n', ' \n'), ('\n', ' '), ('\t', ' '), (',', ', ')]:
# 		string = string.replace(char[0], char[1])
# 	string = dedouble(string, ' ')
# 	return string.strip()

# def dedouble(string: str, char: str):
# 	while char + char[:1] in string:
# 		string = string.replace(char + char[:1], char)
# 	return string

def wrapper(string: str, wrapleft: str, wrapright: str):
	if (string[0], string[-1]) in (("'", "'"), ('[', ']')):
		string = string[1:-1]
	string = string.replace(wrapleft, '').replace(wrapright, '')
	wrap = False
	word = ''
	for letter in string:
		if letter.isalnum() or letter in ('_', '#'):
			if not wrap:
				letter = wrapleft + letter
			wrap = True
		else:
			if wrap:
				letter = wrapright + letter
			wrap = False
		word = word + letter
	if wrap:
		word = word + wrapright
	return word

# def countsubstrings(string: str, substring: str):
# 	return len(string) - len(string.replace(substring, ''))

def is_sql_comment(string: str):
	if len(string) > 1:
		if string[0:2] in ['--', '/*']:
			return True
	else:
		return False

def firstword(string: str):
	if len(string) > 0:
		return string.split()[0]
	else:
		return string
	
def lastword(string: str):
	if len(string) > 0:
		return string.split()[-1]
	else:
		return string
	
# def lastletters(string: str, letters: int = 1):
# 	length = len(string)
# 	if length >= letters:
# 		return string[(-letters % length):length]
# 	else:
# 		return string
	
# def newline_json(json: str):
# 	is_string = False
# 	jsonchars = list(json)
# 	json = ''
# 	for char in jsonchars:
# 		if char == '"':
# 			is_string = not is_string
# 			json += char
# 		elif is_string:
# 			json += char
# 		elif char in ['{', '[']:
# 			json += char + '\n'
# 		elif char in ['}', ']']:
# 			json += '\n' + char + '\n'
# 		elif char == ',':
# 			json += char + '\n'
# 		elif char == ' ':
# 			continue
# 		else:
# 			json += char
# 	return json

# def clean(string: str):
# 	string = string.replace(':', ': ')
# 	string = dedouble(string, ' ')
# 	string = dedouble(string, '\n\n')
# 	for char in (('( ', '('), (' )', ')'), (' :', ':'), (' ,', ',')):
# 		string = string.replace(char[0], char[1])
# 	return string

# def sqlformatterv3(query):
# 	clauses = ['SELECT', 'INTO', 'FROM', 'WHERE', 'HAVING', 'ORDER BY', 'CREATE', 'ALTER', 'UPDATE', 'DELETE', 'DROP', 'MODIFY', 'INSERT', 'VALUES', 'IF', 'ELSE', 'SET']
# 	operators = ['ON', 'AND', 'OR', 'WHEN']
# 	expressions = ['CAST']
# 	datatypes = ['nvarchar']
# 	joins = ['LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'OUTER APPLY', 'CROSS APPLY']

# 	query = reduce(query)

# 	querywords = query.split()
# 	query = ''

# 	for word in querywords:
# 		if '.' in word and not any(expression in word for expression in expressions):
# 			word = wrapper(word, '[', ']')
# 		query = query + ' ' + word

# 	query = suffixnewline_sql(prefixnewline_sql(reduce(query), clauses + operators + joins))

# 	querylines = query.split('\n')
# 	querylines = [line.lstrip() for line in querylines]
# 	query = ''

# 	levels = [QueryLevel(0)]
# 	level = 0

# 	for line in querylines:
# 		start = line.split(' ')[0]
# 		try:
# 			secondinitial = line.split(' ')[1][0]
# 		except:
# 			secondinitial = ''
# 		end = line.rstrip().split(' ')[-1]
# 		if len(end) > 1:
# 			last = end[-1]
# 		else:
# 			last = end

# 		if start not in operators:
# 			if levels[level]._on_ == True and levels[level]._join_ == True:
# 				levels[level]._on_ = False
# 				levels[level]._join_ = False
# 				levels[level].decrement()
# 				levels[level].decrement()
# 			if levels[level]._operator_ == True:
# 				levels[level].decrement()
# 			levels[level]._operator_ = False
# 		elif start in operators and levels[level].operator_ready():
# 			levels[level]._operator_ = True
# 			levels[level].increment()

# 		if start in [')', '),']:
# 			level -= 1
# 			levels.pop()
# 		elif start == 'ON':
# 			levels[level]._on_ = True
# 			if levels[level]._join_:
# 				levels[level].increment()
# 		elif start == 'FROM' and levels[level]._select_:
# 			levels[level]._select_ = False
# 			levels[level].decrement()
# 		elif any(join in line for join in joins):
# 			levels[level]._join_ = True
# 			levels[level].increment()
# 		elif start in ['WHERE', 'HAVING']:
# 			levels[level]._where_ = True

# 		if levels[level].aliasing(line):
# 			line = line[0:len(line) - len(end) - 1] + '[' + end + ']'
		
# 		indents = sum([querylevel.indent for querylevel in levels[0:-1]])
# 		# print('\n', levels, f', last: {last}, end: {end}, operator: {levels[level]._operator_}.', end = '')
# 		line = levels[level].format_line(line, indents)
# 		# print(line, end = '')
# 		query = query + line
# 		# print(levels[level].format_line(line, indents), end = '')

# 		if levels[level]._set_:
# 			levels[level]._set_ = False
# 			levels[level]._on_ = False

# 		if start == 'SET':
# 			levels[level]._set_ = True
# 		elif start == 'SELECT':
# 			levels[level]._select_ = True
# 			levels[level].increment()
# 		elif start == 'VALUES':
# 			levels[level]._values_ = True
# 			levels[level].increment()
# 		elif start == 'CASE':
# 			levels[level]._case_ = True
# 			levels[level].increment()
# 		elif start == 'END' and levels[level]._case_:
# 			levels[level]._case_ = False
# 			levels[level].decrement()

# 		if end == '(':
# 			level += 1
# 			levels.append(QueryLevel(level))
# 		elif levels[level]._operator_ and secondinitial == '(':
# 			level += 1
# 			levels.append(QueryLevel(level))
# 			levels[level]._operator_ = True
# 		elif last == ')':
# 			if levels[level]._values_:
# 				levels[level]._values_ = False
# 				levels[level].decrement()
# 			elif levels[level]._operator_:
# 				level -= 1
# 				levels.pop()

# 	return clean(query)

# def sqlformatterv4(query):
# 	clauses = ['SELECT', 'INTO', 'FROM', 'WHERE', 'HAVING', 'ORDER BY', 'UPDATE', 'CREATE', 'ALTER', 'UPDATE', 'DELETE', 'DROP', 'MODIFY', 'INSERT', 'VALUES', 'ELSE', 'SET', 'WITH', 'GO']
# 	flow = ['BEGIN', 'END', 'IF', 'ELSE']
# 	operators = ['ON', 'AND', 'OR', 'WHEN']
# 	expressions = ['CAST', 'AS', 'CASE', 'IIF', 'NULL', 'DISTINCT']
# 	datatypes = ['nvarchar', 'bit']
# 	joins = ['LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL JOIN', 'CROSS JOIN', 'OUTER APPLY', 'CROSS APPLY']

# 	querychars = list(query.strip())
# 	query = ''
# 	querywords = []
# 	word = ''
# 	alias = 0
# 	comment = False
# 	multicomment = False

# 	for c, char in enumerate(querychars):
# 		if char == '[':
# 			alias += 1
# 		elif char == ']':
# 			alias -= 1
# 		elif c == len(querychars) - 1:
# 			word += char
# 			querywords.append(word)
# 			continue
# 		elif char + querychars[c + 1] == '--' and alias == 0:
# 			comment = True
# 		elif char + querychars[c + 1] == '/*' and alias == 0:
# 			multicomment = True

# 		if comment:
# 			word += char
# 			if char == '\n':
# 				comment = False
# 				querywords.append(word)
# 				word = ''
# 		elif multicomment:
# 			word += char
# 			if querychars[c - 1] + char == '*/':
# 				multicomment = False
# 				querywords.append(word)
# 				word = ''
# 		elif char in (' ', '\n', '\t'):
# 			if alias > 0:
# 				word += char
# 				continue
# 			if querychars[c - 1] in (' ', '\n', '\t'):
# 				continue
# 			querywords.append(word)
# 			word = ''
# 		else:
# 			word += char

# 	print(query)

# 	querywords_loop = [word.strip() if '[' in word else reduce(word) for word in querywords]
# 	querywords = []
# 	_by_ = False
# 	_join_ = False

# 	for c, word in enumerate(querywords_loop):
# 		if '[' in word or is_sql_comment(word):
# 			querywords.append(word)
# 			continue

# 		if _by_:
# 			_by_ = False
# 			continue
		
# 		if _join_:
# 			_join_ = False
# 			continue

# 		if word.upper().replace('(', '') in ['PARTITION', 'GROUP', 'ORDER'] and querywords_loop[c + 1].upper() == 'BY':
# 			_by_ = True
# 			querywords.append(word.upper() + ' BY')
# 			continue
# 		elif word.upper() in [join.split(' ')[0] for join in joins]:
# 			join = word.upper() + ' ' + querywords_loop[c + 1].upper()
# 			if join in joins:
# 				_join_ = True
# 				querywords.append(join)
# 				continue
# 		else:
# 			word = word.replace('(', '( ').replace(')', ' )')
# 			querywords.extend(word.split(' '))

# 	print(querywords)

# 	del querywords_loop

# 	_as_ = False

# 	for c, word in enumerate(querywords):
# 		if len(word) > 1:
# 			letter = word[-1]
# 		else:
# 			letter = word

# 		if '[' in word:
# 			_as_ = False
# 			if letter == ',':
# 				querywords[c] += '\n'
# 			continue

# 		if '.' in word or (_as_ and word.upper() not in clauses + expressions):
# 			querywords[c] = wrapper(word, '[', ']')
# 			_as_ = False

# 		if word.upper() == 'AS':
# 			_as_ = True

# 		if word.upper() in clauses + operators + joins + flow:
# 			querywords[c] = '\n' + word.upper()
# 		elif word.replace('(', '').upper() in expressions:
# 			querywords[c] = word.upper()
# 		elif word.lower() in datatypes:
# 			querywords[c] = word.lower()

# 		if letter == ',' or is_sql_comment(word):
# 			querywords[c] += '\n'

# 	print(querywords)

# 	query = ' '.join(querywords).replace('\n \n', '\n')
# 	querylines = query.split('\n')
# 	query = ''
# 	levels = ['query']
# 	skipcode = '/*--*/'

# 	for c, line in enumerate(querylines):
# 		line = line.strip()
# 		if line == skipcode:
# 			continue
# 		linelength = len(line)
# 		first = firstword(line)
# 		try:
# 			nextfirst = firstword(querylines[c + 1])
# 		except:
# 			nextfirst = ''

# 		_join_ = any(join in line for join in joins)

# 		if first == 'FROM' and levels[-1] == 'SELECT':
# 			levels.pop()
# 		elif _join_:
# 			levels.append('JOIN')
# 		elif first == 'ON' and levels[-1] == 'JOIN':
# 			levels.append('ON')
# 		elif first in operators and levels[-1] == 'ON':
# 			levels.append('logic')
# 		elif first == 'SET' and nextfirst == 'ON':
# 			line += ' ON'
# 			querylines[c + 1] = skipcode
# 		elif first == 'GO':
# 			line += '\n'

# 		if 'OVER' in line:
# 			over = 0
# 			over += countsubstrings(line, '(') - countsubstrings(line, ')')
# 			cc = 1
# 			while over > 0:
# 				nextline = querylines[c + cc]
# 				querylines[c + cc] = skipcode
# 				line += ' ' + nextline
# 				over += countsubstrings(nextline, '(') - countsubstrings(nextline, ')')

# 		query += '\n' + (len(levels) - 1)*'\t' + line
# 		# print(query)

# 		if first in ['WITH', 'SELECT', 'CASE']:
# 			levels.append(first)
# 			if first == 'CASE':
# 				query += ' ' + querylines[c + 1]
# 				querylines[c + 1] = skipcode

# 		if levels[-1] == 'CASE' and first == 'END':
# 			levels.pop()
# 		if levels[-1] == 'logic' and nextfirst not in operators:
# 			levels.pop()
# 		if levels[-1] == 'ON' and nextfirst not in operators:
# 			levels.pop()
# 		if levels[-1] == 'JOIN' and nextfirst != 'ON':
# 			levels.pop()

# 		if lastletters(line, 2) in (' )', '),'):
# 			if countsubstrings(line, '(') - countsubstrings(line, ')') >= 0:
# 				continue
# 			if levels[-1] == 'WITH':
# 				query = query[0:len(query) - 2] 
# 				query += '\n' + (len(levels) - 2)*'\t' + lastletters(line, 2)
# 				if firstword(querylines[c + 1]) != 'SELECT':
# 					query += querylines[c + 1]
# 					querylines[c + 1] = skipcode
# 				else:
# 					levels.pop()
# 				continue
# 			levels.pop()
# 		elif lastletters(line, 1) == '(':
# 			levels.append('subquery')

# 	return clean(query).strip()

def sqlformatterv5(query):
	clauses = ['SELECT', 'INTO', 'FROM', 'WHERE', 'HAVING', 'ORDER BY', 'UPDATE', 
	    'CREATE', 'ALTER', 'UPDATE', 'DELETE', 'DROP', 'MODIFY', 'INSERT', 
		'VALUES', 'ELSE', 'SET', 'WITH', 'GO', 'GROUP BY', 'PARITION BY']
	operators = ['ON', 'AND', 'OR', 'WHEN', 'BEGIN', 'END', 'IF', 'ELSE']
	commands = ['AS', 'CASE', 'NULL', 'DISTINCT', 'THEN', 'OVER', 'IS', 'NOT', 'IN']
	expressions = ['CAST', 'IIF', 'MAX', 'MIN', 'ROW_NUMBER', 'TRIM', 'RTRIM', 
		'LTRIM', 'LEFT', 'RIGHT', 'CONCAT', 'CONCAT_WS', 'CHAR', 'REPLACE', 
		'NULLIF', 'ISNULL', 'GETDATE', 'DATEDIFF', 'DATEADD', 'OBJECT_ID', 'CONVERT']
	datatypes = ['nvarchar', 'bit', 'char']
	joins = ['LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL JOIN', 'CROSS JOIN', 
	  'OUTER APPLY', 'CROSS APPLY']

	skipcode = '/*--*/'

	querywords = []
	word = ''
	multiline_comment = False
	single_comment = False
	sql_object = False
	sql_string = False

	logging.info('Identifying terms in SQL query...')

	for i, char in enumerate(query + '\n'):
		try:
			nextchar = query[i + 1]
		except:
			nextchar = ''

		if not(multiline_comment or single_comment or sql_object or sql_string):
			if char + nextchar == '/*':
				multiline_comment = True
				word += char
			elif char + nextchar == '--':
				single_comment = True
				word += char
			elif char == '[':
				sql_object = True
				word += char
			elif char == "'":
				sql_string = True
				word += char
			elif char in ('(', ')', ',', '*'):
				querywords.append(word)
				word = ''
				querywords.append(char)
			elif char in ('\n', '\t', ' '):
				querywords.append(word)
				word = ''
			else:
				word += char
		else:
			if multiline_comment:
				word += char
				if query[i - 1] + char == '*/':
					multiline_comment = False
					querywords.append(word)
					word = ''
			elif single_comment:
				word += char
				if char == '\n':
					single_comment = False
					querywords.append(word)
					word = ''
			elif sql_object:
				word += char
				if char == ']' and nextchar != '.':
					sql_object = False
					querywords.append(word)
					word = ''
			elif sql_string:
				word += char
				if char == "'" and nextchar != "''":
					sql_string = False
					querywords.append(word)
					word = ''

	# print(querywords)

	querywords_loop = querywords
	querywords = []
	_join_ = False
	_by_ = False
	_as_ = False

	logging.info('Formatting terms in SQL query...')

	for i, word in enumerate(querywords_loop):
		try:
			nextword = querywords_loop[i + 1]
		except:
			nextword = ''

		if word == '' or _join_ or _by_:
			_join_ = False
			_by_ = False
			continue
		elif is_sql_comment(word):
			querywords.append(word)
			continue
		elif word == 'OUTER' and nextword == 'JOIN':
			continue
		elif '.' in word and word[0] != '.' and not word[0].isnumeric():
			word = wrapper(word, '[', ']')
		elif word.upper() + ' ' + nextword.upper() in joins:
			_join_ = True
			word = word.upper() + ' ' + nextword.upper()
		elif word.upper() + ' ' + nextword.upper() in ('PARTITION BY', 'GROUP BY', 'ORDER BY'):
			_by_ = True
			word = word.upper() + ' BY'
		elif word.upper() in clauses + expressions + operators + commands:
			word = word.upper()
		elif word.lower() in datatypes:
			word = word.lower()

		if _as_:
			_as_ = False
			if '[' in word or word in clauses + expressions + operators + commands + joins + datatypes:
				querywords.append(word)
			else:
				querywords.append(wrapper(word, '[', ']'))
		else:
			querywords.append(word)

		if word.upper() == 'AS':
			_as_ = True

	del querywords_loop

	# print(querywords)

	query = ''

	logging.info('Formatting lines in SQL query...')

	for word in querywords:
		if word in clauses + joins + operators + expressions or is_sql_comment(word):
			query += '\n' + word
		elif word == ',':
			query += word + '\n'
		elif word in ('(', ')'):
			query += '\n' + word + '\n'
		else:
			query += ' ' + word

	querylines = query.split('\n')
	query = ''
	levels = ['query']
	prevfirst = ''
	prevlast = ''
	_expression_ = False
	_join_ = False
	_value_ = False

	logging.info('Final formatting of SQL query...')

	for c, line in enumerate(querylines):
		if len(line) == 0 or line == skipcode:
			continue
		line = line.strip()
		first = firstword(line)
		last = lastword(line)
		_join_ = any(join in line for join in joins)

		# print(c, line, levels, _expression_, _join_)
		
		if _expression_:
			_expression_ = False
			if first in commands:
				query += ' ' + line
				prevfirst = first
				prevlast = last
				continue

		if first == 'ON' and prevfirst == 'SET':
			query += ' ' + line
			prevfirst = first
			prevlast = last
			continue

		if levels[-1] == 'logic' and first not in ('AND', 'OR', '('):
			levels.pop()
		if levels[-1] == 'ON' and first not in operators:
			levels.pop()
		if levels[-1] == 'JOIN' and first not in ('ON', '(', 'AS'):
			levels.pop()
		if prevlast == ')' and first != ',' and levels[-1] == 'VALUES':
			levels.pop()

		if first in ('FROM', 'INTO') and levels[-1] == 'SELECT':
			levels.pop()
		elif _join_:
			levels.append('JOIN')
		elif first == 'ON' and levels[-1] == 'JOIN':
			levels.append('ON')
		elif first in ('AND', 'OR') and levels[-1] != 'logic':
			levels.append('logic')

		tabs = len([level for level in levels if level not in expressions + ['query']])

		if line == '(':
			if prevlast in expressions:
				levels.append(prevlast)
				query += line
			elif prevfirst == 'AS' and ']' in prevlast and ',' not in prevlast:
				levels.append('subquery')
				query += line
			else:
				levels.append('subquery')
				if prevfirst in clauses or prevlast in commands + operators or prevfirst + ' ' + prevlast in joins:
					query += ' ' + line
				else:
					query += '\n' + tabs*'\t' + line
		elif line == ')':
			if levels[-1] in expressions:
				_expression_ = True
				query += line
			elif _value_:
				query += line
				_value_ = False
			else:
				query += '\n' + (tabs - 1)*'\t' + line
			levels.pop()
		elif levels[-1] in expressions:
			if prevlast == '(':
				query += line
			else:
				query += ' ' + line
		elif levels[-1] == 'CASE':
			if first in ('WHEN', 'ELSE', 'END') and not prevfirst == 'CASE':
				query += '\n' + tabs*'\t' + line
			else:
				query += ' ' + line
			if first == 'END':
				levels.pop()
		elif first in ('SELECT', 'CASE', 'VALUES'):
			query += '\n' + tabs*'\t' + line
			levels.append(first)
		elif prevlast in (')', '<', '>', '>=', '<=', '<>', '=', "'", '+', '-'):
			if first in commands + expressions or first != ')':
				query += ' ' + line
			else:
				query += '\n' + tabs*'\t' + line
		else:
			if first not in clauses + operators + commands + expressions and prevfirst == '(':
				query += line
				_value_ = True
			elif prevfirst in ('IF', 'ELSE'):
				query += ' ' + line
			else:
				query += '\n' + tabs*'\t' + line

		if last == 'GO':
			query += '\n'
		
		prevfirst = first
		prevlast = last

	while 3*'\n' in query:
		query = query.replace(3*'\n', 2*'\n')

	query = query.replace(8*' ', 4*' ')

	logging.info('Returning formatted query.')

	return query

# def jsonformatterv1(json):
# 	json = reduce(json)
# 	json = newline_json(json)
# 	while '\n\n' in json:
# 		json = json.replace('\n\n', '\n')

# 	logging.info('Inital cleaning of JSON done, now formatting.')
	
# 	level = 0
# 	jsonlines = json.split('\n')
# 	json = ''
# 	for line in jsonlines:
# 		if len(line) > 1:
# 			endchar = line[-1]
# 		else:
# 			endchar = line
		
# 		if line == ',':
# 			json += line
# 		elif endchar in ['{', '[']:
# 			json += '\n' + level*'\t' + line
# 			level += 1
# 		elif endchar in ['}', ']']:
# 			level -= 1
# 			json += '\n' + level*'\t' + line
# 		else:
# 			json += '\n' + level*'\t' + line

# 	logging.info('Cleaning and returning formatted JSON.')

# 	return clean(json)

def jsonformatterv2(json: str):
	logging.info('Cleaning JSON and identifying terms...')
	json = json.replace('\n', '')
	json = json.replace('\t', '')
	jsonwords = []
	word = ''
	_string_ = False
	for char in json:
		if char == '"' and not _string_:
			_string_ = True
			word += char
		elif _string_:
			word += char
			if char == '"':
				_string_ = False
				jsonwords.append(word)
				word = ''
		elif char in ('{', '}', '[', ']', ':', ','):
			jsonwords.append(word)
			word = ''
			jsonwords.append(char)
		elif char == ' ':
			continue
		else:
			word += char

	logging.info('Formatting the JSON...')
	
	json = ''
	levels = []
	_value_ = False
	for word in jsonwords:
		if len(word) == 0:
			continue
		elif word in ('{', '['):
			if _value_:
				_value_ = False
				json += word
			else:
				json += '\n' + len(levels)*'\t' + word
			if word == '{':
				levels.append('object')
			else:
				levels.append('array')
		elif word in ('}', ']'):
			_value_ = False
			levels.pop()
			json += '\n' + len(levels)*'\t' + word
		elif word == ':':
			_value_ = True
			json += word + ' '
		elif _value_ or word == ',':
			_value_ = False
			json += word
		else:
			json += '\n' + len(levels)*'\t' + word

	logging.info('Returning formatted JSON.')
	
	return json

def dataframe_to_html(df: pd.DataFrame, gradient_cols: list = [], colours: dict = json.load(open("pyjap/config.json"))["personal"]):
	col_info = {}
	for col in gradient_cols:
		col_info[col] = {}
		min = df[col].min(skipna=True)
		if np.isnan(min):
			col_info[col]['min'] = 0
		else:
			col_info[col]['min'] = min
		max = df[col].max(skipna=True)
		if np.isnan(max):
			col_info[col]['max'] = 0
		else:
			col_info[col]['max'] = max
		col_info[col]['signed'] = (col_info[col]['min'] < 0)
	df[gradient_cols] = df[gradient_cols].fillna(0)
	df = df.fillna('')
	html = ""
	html += f'<table style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid {colours["black"]};border-collapse:collapse">\n'
	html += f'\t<tr style="color:{colours["white"]}">\n\t\t<th style="background-color:{colours["dark-accent"]};border:2px solid {colours["black"]}">{df.index.name}</th>\n'
	for header in df.columns:
		html += f'\t\t<th style="background-color:{colours["main"]};border:2px solid {colours["black"]}">{header}</th>\n'
	html += '\t</tr>\n'
	for i, (index, row) in enumerate(df.iterrows()):
		html += f'\t</tr>\n\t\t<td style="border:2px solid {colours["black"]};background-color:{colours["dark-accent"]};color:{colours["white"]}">{str(index)}</td>\n'
		for col, value in row.to_dict().items():
			if col in gradient_cols and value >= col_info[col]['min'] and value <= col_info[col]['max']:
				if col_info[col]['signed']:
					fontcolour = colours["white"]
					if value < 0:
						background = utilities.gradient_hex(value, col_info[col]['min'], 0, colours["negative"], colours["null"])
					else:
						background = utilities.gradient_hex(value, col_info[col]['max'], 0, colours["positive"], colours["null"])
				else:
					background = utilities.gradient_hex(value, col_info[col]['min'], col_info[col]['max'], colours["white"], colours["light-accent"])
					fontcolour = colours["black"]
			else:
				if i % 2 == 0:
					background = colours["white"]
				else:
					background = colours["grey"]
				fontcolour = colours["black"]
			html += f'\t\t<td style="border:1px solid {colours["black"]};background-color:{background};color:{fontcolour}">{str(value)}</td>\n'
		html += '\t</tr>\n'
	html += '</table>'
	return html
