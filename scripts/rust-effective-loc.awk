# Counts non-blank Rust source lines after removing comments. Rust block comments may nest, so a
# regular expression alone is not sufficient. String and raw-string states prevent comment markers
# inside literals from changing the comment state.

function raw_start_at(source, offset,    cursor, prefix, hashes) {
  raw_start_length = 0
  raw_start_hashes = -1
  prefix = substr(source, offset, 1)
  if (prefix == "r") {
    cursor = offset + 1
  } else if ((prefix == "b" || prefix == "c") && substr(source, offset + 1, 1) == "r") {
    cursor = offset + 2
  } else {
    return 0
  }

  hashes = 0
  while (substr(source, cursor, 1) == "#") {
    hashes++
    cursor++
  }
  if (substr(source, cursor, 1) != "\"") {
    return 0
  }

  raw_start_length = cursor - offset + 1
  raw_start_hashes = hashes
  return 1
}

function raw_end_at(source, offset, hashes,    hash_index) {
  if (substr(source, offset, 1) != "\"") {
    return 0
  }
  for (hash_index = 1; hash_index <= hashes; hash_index++) {
    if (substr(source, offset + hash_index, 1) != "#") {
      return 0
    }
  }
  return 1
}

BEGIN {
  block_depth = 0
  in_string = 0
  string_escape = 0
  raw_hashes = -1
  effective_lines = 0
}

{
  source = $0
  length_of_line = length(source)
  has_code = 0
  offset = 1

  while (offset <= length_of_line) {
    current = substr(source, offset, 1)
    following = substr(source, offset + 1, 1)

    if (raw_hashes >= 0) {
      has_code = 1
      if (raw_end_at(source, offset, raw_hashes)) {
        offset += raw_hashes + 1
        raw_hashes = -1
      } else {
        offset++
      }
      continue
    }

    if (in_string) {
      has_code = 1
      if (string_escape) {
        string_escape = 0
      } else if (current == "\\") {
        string_escape = 1
      } else if (current == "\"") {
        in_string = 0
      }
      offset++
      continue
    }

    if (block_depth > 0) {
      if (current == "/" && following == "*") {
        block_depth++
        offset += 2
      } else if (current == "*" && following == "/") {
        block_depth--
        offset += 2
      } else {
        offset++
      }
      continue
    }

    if (current == "/" && following == "/") {
      break
    }
    if (current == "/" && following == "*") {
      block_depth++
      offset += 2
      continue
    }
    if (raw_start_at(source, offset)) {
      has_code = 1
      raw_hashes = raw_start_hashes
      offset += raw_start_length
      continue
    }
    if (current == "\"") {
      has_code = 1
      in_string = 1
      string_escape = 0
      offset++
      continue
    }
    if (current ~ /[^[:space:]]/) {
      has_code = 1
    }
    offset++
  }

  if (has_code) {
    effective_lines++
  }
}

END {
  print effective_lines
}
