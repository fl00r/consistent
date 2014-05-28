# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'consistent/version'

Gem::Specification.new do |spec|
  spec.name          = "consistent"
  spec.version       = Consistent::VERSION
  spec.authors       = ["Petr Yanovich"]
  spec.email         = ["fl00r@yandex.ru"]
  spec.summary       = %q{consistent}
  spec.description   = %q{consistent}
  spec.homepage      = ""
  spec.license       = "MAIL.RU"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]
  spec.extensions    = ["ext/extconf.rb"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
end
