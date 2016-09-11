package com.cloudera.labs.envelope.plan;

/**
 * The type of mutation being planned.
 */
public enum MutationType {
    
    // Do not apply this mutation.
    NONE
    
    // Insert the mutation as new rows. Do not impact existing rows.
  , INSERT
  
    // Update the matching existing rows with the values of the mutation. Do not add new rows.
  , UPDATE
  
    // Delete the existing rows that match the mutation. May contain non-key fields.
  , DELETE
  
    // Insert or update the mutation based on whether the key of the mutation already exists.
  , UPSERT
  
    // Replace all existing rows with the mutation.
  , OVERWRITE
  
}
