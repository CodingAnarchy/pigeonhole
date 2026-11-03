# frozen_string_literal: true

# The API snapshot check (ICR 0111): every public class under `Pigeonhole` and its public
# methods (`public_instance_methods(false)`, `singleton_methods(false)`) match the
# declarations in sig/pigeonhole.rbs, so a method can't be added or dropped without its
# signature. `Pigeonhole::Native` holds the scan cursor's internals and is not public API.
require_relative "test_helper"
require "rbs"

class TestSignatures < Minitest::Test
  SIG = File.expand_path("../sig/pigeonhole.rbs", __dir__)

  # Declared methods per class name: {"Pigeonhole::Table" => {instance: Set, singleton: Set}}.
  def declared
    _, _, decls = RBS::Parser.parse_signature(File.read(SIG))
    out = Hash.new { |h, k| h[k] = { instance: [], singleton: [] } }
    walk = lambda do |decl, outer|
      name = [outer, decl.name.to_s].compact.join("::")
      out[name] # every declared class or module, even without methods
      decl.members.each do |m|
        case m
        when RBS::AST::Declarations::Class, RBS::AST::Declarations::Module
          walk.call(m, name)
        when RBS::AST::Members::MethodDefinition
          next if m.visibility == :private || m.name == :initialize

          out[name][:instance] << m.name if %i[instance singleton_instance].include?(m.kind)
          out[name][:singleton] << m.name if %i[singleton singleton_instance].include?(m.kind)
        when RBS::AST::Members::AttrReader, RBS::AST::Members::AttrAccessor
          out[name][:instance] << m.name
        when RBS::AST::Members::Alias
          out[name][m.kind == :singleton ? :singleton : :instance] << m.new_name
        end
      end
    end
    decls.each { |d| walk.call(d, nil) if d.respond_to?(:members) }
    out.transform_values { |v| v.transform_values { |names| names.map(&:to_sym).sort } }
  end

  # The public classes and modules under Pigeonhole, by name.
  def actual
    out = { "Pigeonhole" => Pigeonhole }
    Pigeonhole.constants.sort.each do |c|
      v = Pigeonhole.const_get(c)
      out["Pigeonhole::#{c}"] = v if v.is_a?(Module) && v != Pigeonhole::Native
    end
    out
  end

  def test_signatures_parse_and_resolve
    loader = RBS::EnvironmentLoader.new
    loader.add(path: Pathname(File.dirname(SIG)))
    env = RBS::Environment.from_loader(loader).resolve_type_names
    builder = RBS::DefinitionBuilder.new(env: env)
    actual.each do |name, mod|
      type = RBS::TypeName.parse("::#{name}")
      refute_nil builder.build_instance(type), name if mod.is_a?(Class)
      refute_nil builder.build_singleton(type), name
    end
  end

  def test_every_public_class_is_declared_and_no_more
    assert_equal actual.keys.sort, declared.keys.sort
  end

  # `rbs test`'s hooks (RBS_TEST_TARGET) alias each checked method; they aren't API.
  RBS_HOOK = /__with(out)?__RBS_TEST_/

  def test_every_public_method_is_declared_and_no_more
    sigs = declared
    actual.each do |name, mod|
      instance = mod.is_a?(Class) ? mod.public_instance_methods(false) : []
      instance = instance.grep_v(RBS_HOOK).sort
      singleton = mod.singleton_methods(false).grep_v(RBS_HOOK).sort
      assert_equal instance, sigs[name][:instance], "#{name}: instance methods vs sig/pigeonhole.rbs"
      assert_equal singleton, sigs[name][:singleton],
                   "#{name}: singleton methods vs sig/pigeonhole.rbs"
    end
  end
end
