require 'mkmf'
find_header("consistent.h")
extension_name = "consistent_ring"
dir_config(extension_name)
create_makefile(extension_name)