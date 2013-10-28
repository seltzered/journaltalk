class Keystore < ActiveRecord::Base
  validates_presence_of :key

  attr_accessible nil

  def self.get(key)
    Keystore.find_by_key(key)
  end

  def self.put(key, value)
    key_column = Keystore.connection.quote_column_name("key")
    value_column = Keystore.connection.quote_column_name("value")

    if Keystore.connection.adapter_name == "SQLite"
      Keystore.connection.execute("INSERT OR REPLACE INTO " <<
        "#{Keystore.table_name} (#{key_column}, #{value_column}) VALUES " <<
        "(#{q(key)}, #{q(value)})")
    
    elsif Keystore.connection.adapter_name == "PostgreSQL"
      Keystore.connection.execute("UPDATE #{Keystore.table_name} " +
        "SET #{value_column} =#{q(value)} WHERE #{key_column} =#{q(key)}")
      Keystore.connection.execute("INSERT INTO #{Keystore.table_name} (#{key_column}, #{value_column}) " +
        "SELECT #{q(key)}, #{q(value)} " +
        "WHERE NOT EXISTS (SELECT 1 FROM #{Keystore.table_name} WHERE #{key_column} = #{q(key)}) "
        )

    elsif Keystore.connection.adapter_name == "MySQL" || Keystore.connection.adapter_name == "Mysql2"
      Keystore.connection.execute("INSERT INTO #{Keystore.table_name} (" +
        "#{key_column}, #{value_column}) VALUES (#{q(key)}, #{q(value)}) ON DUPLICATE KEY " +
        "UPDATE #{value_column} = #{q(value)}")

    else
      raise "Error: keystore requires db-specific put method."

    end

    true
  end

  def self.increment_value_for(key, amount = 1)
    self.incremented_value_for(key, amount)
  end

  def self.incremented_value_for(key, amount = 1)
    new_value = nil

    Keystore.transaction do    

      key_column = Keystore.connection.quote_column_name("key")
      value_column = Keystore.connection.quote_column_name("value")


      if Keystore.connection.adapter_name == "SQLite"
        Keystore.connection.execute("INSERT OR IGNORE INTO " <<
          "#{Keystore.table_name} (#{key_column}, #{value_column}) VALUES " <<
          "(#{q(key)}, 0)")
        Keystore.connection.execute("UPDATE #{Keystore.table_name} " <<
          "SET #{value_column} = #{value_column} + #{q(amount)} WHERE #{key_column} = #{q(key)}")
      
      elsif Keystore.connection.adapter_name == "PostgreSQL"
        previous_keystore = Keystore.find_by_key(key)
        if(previous_keystore == nil || previous_keystore.value == nil)
          previous_amount = 0
        else
          previous_amount = previous_keystore.value
        end

        Keystore.connection.execute("UPDATE #{Keystore.table_name} " +
          "SET #{value_column}=#{q(previous_amount)} + #{q(amount)} WHERE #{key_column}=#{q(key)}")
        Keystore.connection.execute("INSERT INTO #{Keystore.table_name} (#{key_column}, #{value_column}) " +
          "SELECT #{q(key)}, #{q(previous_amount)} + #{q(amount)} " +
          "WHERE NOT EXISTS (SELECT 1 FROM #{Keystore.table_name} WHERE #{key_column}=#{q(key)}) ")
      
      elsif Keystore.connection.adapter_name == "MySQL" || Keystore.connection.adapter_name == "Mysql2"
        Keystore.connection.execute("INSERT INTO #{Keystore.table_name} (" +
          "#{key_column}, #{value_column}) VALUES (#{q(key)}, #{q(amount)}) ON DUPLICATE KEY " +
          "UPDATE #{value_column} = #{value_column} + #{q(amount)}")
      
      else
        raise "Error: keystore requires db-specific increment method."

      end

      new_value = self.value_for(key)
    end

    return new_value
  end

  def self.decrement_value_for(key, amount = -1)
    self.increment_value_for(key, amount)
  end

  def self.decremented_value_for(key, amount = -1)
    self.incremented_value_for(key, amount)
  end

  def self.value_for(key)
    self.get(key).try(:value)
  end

end
