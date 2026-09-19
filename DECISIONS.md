# `DECISIONS`
### A. The API can create a database and then lose the response. How does your controller avoid creating a duplicate on the next reconcile? What risk remains that you did not eliminate?

**ANSWER:** I caught an internal service error produced by the provisioning client, updated the resource with the state 'UNKNOWN', and added an appropriate annotation to inform the user. Then this user can get the DB ID from the provider console and manually update the resource with this ID by command:
```sh
kubectl patch manageddatabases.tt.yagatito.com orders \
  --subresource=status \
  --type=merge \
  -p '{"status":{"id":"db-b0cf16a6"}}'
```
Then the worker should catch this update and handle it in a common way.
<br>

### B. What would you change about this external API to make your job easier? How would you argue for it to the team that owns it and has its own backlog?

**ANSWER:** I would add some idempotency header/key support in order to not have such cases when the user has to manually patch a resource, and the 4th state (UNKNOWN) will be gone so the worker could be simplified.
<br>

### C. Deleting the external database can keep failing. Do you block deletion of the custom resource indefinitely, or give up at some point and let it go? Justify the choice you made.

**ANSWER:** I think it is better to have some counter of tries and its limit. And when the limit is exceeded, mark this resource so it will be manually checked in the provider console and investigated. Anyway, spamming retries could be better than letting it go just because the second option can be much more expensive!
<br>

### D. Someone edits `sizeGB` after the database exists. The API has no resize operation. What does your controller do, and what does the user see?
**ANSWER:** By the current implementation, such updates are ignored and the user is able to see the condition block in the result of the 'describe' command.
<br>

### E. What did you deliberately leave out, and what would you do next?
**ANSWER:** At first, remove the informing annotation after the manual patch was applied (after the internal service error from the provider API). And after all, `entity.go` file needs some refactoring on which I don't have time, currently. And there is also should be rewrite condition mechanism.
<br>