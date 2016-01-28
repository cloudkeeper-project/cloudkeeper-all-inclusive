package com.svbio.workflow.entities;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Entity
@DiscriminatorValue("SIMPLE")
public class SimpleProperties extends ProcessLauncherProperties<SimpleProperties> { }
