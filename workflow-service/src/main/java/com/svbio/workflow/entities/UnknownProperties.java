package com.svbio.workflow.entities;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("UNKNOWN")
public class UnknownProperties extends ExecutionFrameProperties<UnknownProperties> { }
